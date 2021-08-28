package sqs

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/service/sqs"
)

const (
	attrNumberOfMessages  string = "ApproximateNumberOfMessages"
	attrNotVisible        string = "ApproximateNumberOfMessagesNotVisible"
	attrRedrivePolicy     string = "RedrivePolicy"
	attrQueueArn          string = "QueueArn"
	attrVisibilityTimeout string = "VisibilityTimeout"
	dataTypeString        string = "String"
)

type Sqs struct {
	client *sqs.SQS
}

type redrivePolicy struct {
	TargetArn       string `json:"deadLetterTargetArn"`
	MaxReceiveCount int    `json:"maxReceiveCount"`
}

func (p *redrivePolicy) getNameOrTargetArn(queueArn string) string {
	account, region, _ := parseArn(queueArn)
	a, r, name := parseArn(p.TargetArn)
	if a == account && r == region {
		return name
	}
	return p.TargetArn
}

type QueueInfo struct {
	QueueName          string
	QueueUrl           string
	NumberOfMessages   int
	MessagesNotVisible int
	VisibilityTimeout  int
	DeadLetterTarget   string
	MaxReceiveCount    int
}

func (o *QueueInfo) String() string {
	return fmt.Sprintf("%s\t%d\t%d\t%d\t%d\t%s",
		o.QueueName,
		o.NumberOfMessages,
		o.MessagesNotVisible,
		o.VisibilityTimeout,
		o.MaxReceiveCount,
		o.DeadLetterTarget)
}

func newQueueInfo(name string, attributes map[string]*string) *QueueInfo {
	out := &QueueInfo{
		QueueName: name,
	}

	if nom, err := strconv.Atoi(*attributes[attrNumberOfMessages]); err == nil {
		out.NumberOfMessages = nom
	}
	if mnv, err := strconv.Atoi(*attributes[attrNotVisible]); err == nil {
		out.MessagesNotVisible = mnv
	}
	if vt, err := strconv.Atoi(*attributes[attrVisibilityTimeout]); err == nil {
		out.VisibilityTimeout = vt
	}

	if p, ok := attributes[attrRedrivePolicy]; ok {
		var policy redrivePolicy
		if err := json.Unmarshal([]byte(*p), &policy); err == nil {
			out.DeadLetterTarget = policy.getNameOrTargetArn(*attributes[attrQueueArn])
			out.MaxReceiveCount = policy.MaxReceiveCount
		}
	}

	return out
}

type QueueUrl string

func (q *QueueUrl) Name() string {
	parts := strings.Split(string(*q), "/")
	return parts[len(parts)-1]
}

func New(s *session.Session) *Sqs {
	return &Sqs{
		client: sqs.New(s),
	}
}

func (s *Sqs) List() ([]QueueUrl, error) {

	inp := &sqs.ListQueuesInput{
		MaxResults:      aws.Int64(1000),
		QueueNamePrefix: aws.String(""),
	}

	resp, err := s.client.ListQueues(inp)
	if err != nil {
		return nil, errors.Wrap(err, "ListQueues failed")
	}

	// todo: paging if more then 1000
	urls := make([]QueueUrl, len(resp.QueueUrls))
	for c, u := range resp.QueueUrls {
		urls[c] = QueueUrl(*u)
	}

	return urls, nil
}

func (s *Sqs) GetQueueUrl(name string) (string, error) {
	out, err := s.client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		return "", errors.Wrap(err, "GetQueueUrl failed")
	}
	return *out.QueueUrl, nil

}

func (i *Sqs) Info(queueNames []string) ([]*QueueInfo, error) {
	ch := make(chan *QueueInfo)
	cherr := make(chan error)

	for _, n := range queueNames {
		go i.getInfo(n, ch, cherr)
	}

	output := make([]*QueueInfo, len(queueNames))
	for c, _ := range queueNames {
		output[c] = <-ch
	}
	sort.Slice(output, func(i, j int) bool {
		return output[i].QueueName < output[j].QueueName
	})
	return output, nil

}

func (s *Sqs) getInfo(name string, ch chan<- *QueueInfo, cherr chan<- error) {
	out, err := s.client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		cherr <- errors.Wrap(err, "GetQueueUrl failed")
	}

	resp, err := s.client.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
	})
	if err != nil {
		cherr <- errors.Wrap(err, "GetQueueAttributes failed")
	}

	ch <- newQueueInfo(name, resp.Attributes)
}

func (i *Sqs) Move(src, dst string, limit int) (int, error) {
	var (
		srcUrl string = src
		dstUrl string = dst
		err    error
	)
	if isUrl(src) == false {
		srcUrl, err = i.GetQueueUrl(src)
		if err != nil {
			return 0, err
		}
	}
	if isUrl(dst) == false {
		dstUrl, err = i.GetQueueUrl(dst)
		if err != nil {
			return 0, err
		}
	}
	return i.move(srcUrl, dstUrl, limit)
}

func (s *Sqs) move(srcUrl, dstUrl string, limit int) (int, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(srcUrl),
		VisibilityTimeout:     aws.Int64(5),
		MaxNumberOfMessages:   aws.Int64(10),
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
	}

	processed := 0
	for {
		receive, err := s.client.ReceiveMessage(input)
		if err != nil {
			return processed, errors.Wrap(err, "ReceiveMessage failed")
		}

		if len(receive.Messages) == 0 || processed == limit {
			return processed, nil
		}

		messages := receive.Messages

		if len(receive.Messages)+processed > limit {
			messages = receive.Messages[0 : limit-processed]
		}

		if len(messages) == 0 {
			return processed, nil
		}

		send, err := s.client.SendMessageBatch(&sqs.SendMessageBatchInput{
			QueueUrl: aws.String(dstUrl),
			Entries:  getEntries(messages),
		})

		if err != nil {
			return processed, errors.Wrap(err, "SendMessageBatch failed")
		}

		if len(send.Failed) > 0 {
			return processed, fmt.Errorf("failed to send %s messages", len(send.Failed))
		}

		if len(send.Successful) == len(messages) {
			del, err := s.client.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
				Entries:  getDeleteEntries(messages),
				QueueUrl: aws.String(srcUrl),
			})

			if err != nil {
				return processed, errors.Wrap(err, "DeleteMessageBatch failed")
			}

			if len(del.Failed) > 0 {
				return processed, fmt.Errorf("failed to delete %s messages", len(del.Failed))
			}
			processed += len(messages)
		}
	}
	return processed, nil
}

func (s *Sqs) Download(src, dst string, count int, del bool) (int, error) {
	var (
		srcUrl string = src
		err    error
	)
	if isUrl(src) == false {
		srcUrl, err = s.GetQueueUrl(src)
		if err != nil {
			return 0, err
		}
	}
	return s.download(srcUrl, dst, count, del)
}

func (s *Sqs) download(src, dst string, limit int, del bool) (int, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(src),
		VisibilityTimeout:     aws.Int64(2),
		MaxNumberOfMessages:   aws.Int64(10),
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
	}

	processed := 0
	for {
		receive, err := s.client.ReceiveMessage(input)
		if err != nil {
			return processed, errors.Wrap(err, "ReceiveMessage failed")
		}

		if len(receive.Messages) == 0 || processed == limit {
			return processed, nil
		}

		messages := receive.Messages

		if len(receive.Messages)+processed > limit {
			messages = receive.Messages[0 : limit-processed]
		}

		if len(messages) == 0 {
			return processed, nil
		}

		for _, msg := range messages {
			fp := path.Join(dst, fmt.Sprintf("%s.json", *msg.MessageId))
			f, err := os.Create(fp)
			defer f.Close()
			if err != nil {
				return processed, errors.Wrap(err, fmt.Sprintf("Cannot create file: %s", fp))
			}
			err = json.NewEncoder(f).Encode(msg)
			if err != nil {
				return processed, errors.Wrap(err, "json.Marshal failed")
			}

		}
		if del {
			delBatch, err := s.client.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
				Entries:  getDeleteEntries(messages),
				QueueUrl: aws.String(src),
			})

			if err != nil {
				return processed, errors.Wrap(err, "DeleteMessageBatch failed")
			}

			if len(delBatch.Failed) > 0 {
				return processed, fmt.Errorf("failed to delete %s messages", len(delBatch.Failed))
			}
		}
		processed += len(messages)
	}

	return processed, nil
}

func (s *Sqs) Send(dst, body string, attributes map[string]string) (string, error) {
	var (
		dstUrl string = dst
		err    error
	)
	if isUrl(dst) == false {
		dstUrl, err = s.GetQueueUrl(dst)
		if err != nil {
			return "", err
		}
	}
	return s.send(dstUrl, body, attributes)
}

func (s *Sqs) send(dstUrl, body string, attributes map[string]string) (string, error) {
	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(dstUrl),
		MessageBody: aws.String(body),
	}
	if len(attributes) > 0 {
		ma := make(map[string]*sqs.MessageAttributeValue, len(attributes))
		for k, v := range attributes {
			ma[k] = &sqs.MessageAttributeValue{
				DataType:    aws.String(dataTypeString),
				StringValue: aws.String(v),
			}
		}
		input.MessageAttributes = ma
	}
	resp, err := s.client.SendMessage(input)
	if err != nil {
		return "", errors.Wrap(err, "SendMessage failed")
	}
	return *resp.MessageId, nil
}

func getEntries(messages []*sqs.Message) []*sqs.SendMessageBatchRequestEntry {
	entries := make([]*sqs.SendMessageBatchRequestEntry, len(messages))
	for i, message := range messages {
		re := &sqs.SendMessageBatchRequestEntry{
			MessageBody:       message.Body,
			Id:                message.MessageId,
			MessageAttributes: message.MessageAttributes,
		}

		if messageGroupId, ok := message.Attributes[sqs.MessageSystemAttributeNameMessageGroupId]; ok {
			re.MessageGroupId = messageGroupId
		}

		if messageDeduplicationId, ok := message.Attributes[sqs.MessageSystemAttributeNameMessageDeduplicationId]; ok {
			re.MessageDeduplicationId = messageDeduplicationId
		}

		entries[i] = re
	}

	return entries
}

func getDeleteEntries(messages []*sqs.Message) []*sqs.DeleteMessageBatchRequestEntry {
	entries := make([]*sqs.DeleteMessageBatchRequestEntry, len(messages))
	for i, message := range messages {
		entries[i] = &sqs.DeleteMessageBatchRequestEntry{
			ReceiptHandle: message.ReceiptHandle,
			Id:            message.MessageId,
		}
	}

	return entries
}

// parseArn returns account number, region and queue name for a given arn
func parseArn(arn string) (string, string, string) {
	parts := strings.Split(arn, ":")
	return parts[4], parts[3], parts[5]
}

func isUrl(s string) bool {
	if _, err := url.ParseRequestURI(s); err == nil {
		return true
	}
	return false
}
