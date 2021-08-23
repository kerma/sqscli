package sqs

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/service/sqs"
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

type GetOutput struct {
	QueueName          string
	NumberOfMessages   int
	MessagesNotVisible int
	VisibilityTimeout  int
	DeadLetterTarget   string
	MaxReceiveCount    int
}

func NewGetOutput(name string, attr map[string]*string) *GetOutput {
	out := &GetOutput{
		QueueName: name,
	}

	if nom, err := strconv.Atoi(*attr["ApproximateNumberOfMessages"]); err == nil {
		out.NumberOfMessages = nom
	}
	if mnv, err := strconv.Atoi(*attr["ApproximateNumberOfMessagesNotVisible"]); err == nil {
		out.NumberOfMessages = mnv
	}
	if vt, err := strconv.Atoi(*attr["VisibilityTimeout"]); err == nil {
		out.VisibilityTimeout = vt
	}

	if p, ok := attr["RedrivePolicy"]; ok {
		fmt.Println(*p)
		var policy redrivePolicy
		if err := json.Unmarshal([]byte(*p), &policy); err == nil {
			out.DeadLetterTarget = policy.getNameOrTargetArn(*attr["QueueArn"])
			out.MaxReceiveCount = policy.MaxReceiveCount
		}
	}

	return out
}

func (o *GetOutput) String() string {
	return fmt.Sprintf("%s\t%d\t%d\t%d\t%d\t%s",
		o.QueueName,
		o.NumberOfMessages,
		o.MessagesNotVisible,
		o.VisibilityTimeout,
		o.MaxReceiveCount,
		o.DeadLetterTarget)
}

func New(s *session.Session) *Sqs {
	return &Sqs{
		client: sqs.New(s),
	}
}

type QueueUrl string

func (q *QueueUrl) Name() string {
	parts := strings.Split(string(*q), "/")
	return parts[len(parts)-1]
}

func (i *Sqs) List() ([]QueueUrl, error) {

	inp := &sqs.ListQueuesInput{
		MaxResults:      aws.Int64(1000),
		QueueNamePrefix: aws.String(""),
	}

	resp, err := i.client.ListQueues(inp)
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

func (i *Sqs) Get(queueNames []string) ([]*GetOutput, error) {
	ch := make(chan *GetOutput)
	cherr := make(chan error)

	for _, n := range queueNames {
		go i.get(n, ch, cherr)
	}

	output := make([]*GetOutput, len(queueNames))
	for c, _ := range queueNames {
		output[c] = <-ch
	}
	return output, nil

}

func (i *Sqs) get(name string, ch chan<- *GetOutput, cherr chan<- error) {
	out, err := i.client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		cherr <- errors.Wrap(err, "GetQueueUrl failed")
	}

	resp, err := i.client.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl:       out.QueueUrl,
		AttributeNames: []*string{aws.String("All")},
	})
	if err != nil {
		cherr <- errors.Wrap(err, "GetQueueAttributes failed")
	}

	ch <- NewGetOutput(name, resp.Attributes)
}

// parseArn returns account number, region and queue name for a given arn
func parseArn(arn string) (string, string, string) {
	parts := strings.Split(arn, ":")
	return parts[4], parts[3], parts[5]
}
