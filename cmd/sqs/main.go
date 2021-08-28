package main

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"

	sqs "github.com/kerma/sqscli"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/spf13/cobra"
)

type Options struct {
	debug bool
}

var opts = &Options{
	debug: false,
}

var rootCmd = &cobra.Command{
	Use:   "sqs",
	Short: "sqs is a CLI for AWS SQS",
}

func listCommand(opts *Options) *cobra.Command {
	var (
		one  bool
		urls bool
	)

	client := sqs.New(session.Must(session.NewSession()))

	simpleList := func() error {
		list, err := client.List()
		if err != nil {
			return err
		}
		if len(list) == 0 {
			fmt.Println("No queues")
			return nil
		}
		for _, url := range list {
			if urls {
				fmt.Println(url)
			} else {
				fmt.Println(url.Name())
			}
		}
		return nil
	}

	cmd := &cobra.Command{
		Use:   "ls [flags] [queueNames]",
		Short: "list queues",
		RunE: func(cmd *cobra.Command, args []string) error {

			if len(args) == 0 && one {
				return simpleList()
			}

			var queueNames []string
			if len(args) == 0 {
				list, err := client.List()
				if err != nil {
					return err
				}
				if len(list) == 0 {
					fmt.Println("No queues")
					return nil
				}
				for _, val := range list {
					queueNames = append(queueNames, val.Name())
				}
			} else {
				queueNames = args
			}

			results, err := client.Info(queueNames)
			if err != nil {
				return err
			}

			if len(results) == 0 {
				fmt.Println("No queues")
				return nil
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "NAME\tMESSAGES\tIN-FLIGHT\tTIMEOUT\tMAX\tDEAD-LETTER-TARGET")
			for _, val := range results {
				fmt.Fprintln(w, val.String())
			}
			w.Flush()

			return nil
		},
	}
	cmd.Flags().BoolVarP(&one, "one", "1", false, "List queues only. Effective when no queue names provided")
	cmd.Flags().BoolVarP(&urls, "urls", "u", false, "List as URLs. Effective with -1")
	return cmd
}

func moveCommand(opts *Options) *cobra.Command {
	var (
		limit int
	)
	cmd := &cobra.Command{
		Use:   "mv [flags] sourceQueue destinationQueue",
		Short: "Move messages from one queue to another",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			src, dest := args[0], args[1]

			client := sqs.New(session.Must(session.NewSession()))

			attrs, err := client.Info([]string{src})
			if err != nil {
				return err
			}

			if limit > 0 {
				fmt.Printf("Moving %d out of %d messages, please wait...\n", limit, attrs[0].NumberOfMessages)
			} else {
				fmt.Printf("Move %d messages, please wait...\n", attrs[0].NumberOfMessages)
			}

			moved, err := client.Move(src, dest, limit)
			fmt.Printf("Moved %d messages\n", moved)

			if err != nil {
				return err
			}

			return nil
		},
	}
	cmd.Flags().IntVarP(&limit, "limit", "l", 0, "Limits number of messages moved")
	return cmd
}

func sendCommand(opts *Options) *cobra.Command {
	var (
		attrs map[string]string
	)
	cmd := &cobra.Command{
		Use:   "send [flags] queue < body.txt",
		Short: "Send a message to queue",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := sqs.New(session.Must(session.NewSession()))
			id, err := client.Send(args[0], readFromStdin(), attrs)
			if err != nil {
				return err
			}
			fmt.Println("Sent message id:", id)
			return nil
		},
	}
	cmd.Flags().StringToStringVarP(&attrs, "attributes", "a", make(map[string]string, 0), "key=value pair of message attributes")
	return cmd
}

func downloadCommand(opts *Options) *cobra.Command {
	var (
		del   bool
		limit int
	)
	cmd := &cobra.Command{
		Use:   "download [flags] queue destinationDir",
		Short: "Download messages from queue",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := sqs.New(session.Must(session.NewSession()))
			downloaded, err := client.Download(args[0], args[1], limit, del)
			fmt.Printf("Downloaded %d messages\n", downloaded)
			if err != nil {
				return err
			}
			return nil
		},
	}
	cmd.Flags().BoolVarP(&del, "delete", "d", false, "Delete the message from the queue")
	cmd.Flags().IntVarP(&limit, "limit", "l", 1, "Number of messages to download")
	return cmd
}

func readFromStdin() string {
	scanner := bufio.NewScanner(os.Stdin)
	builder := strings.Builder{}
	for scanner.Scan() {
		builder.Write(scanner.Bytes())
	}
	return builder.String()
}

func isUrl(s string) bool {
	if _, err := url.ParseRequestURI(s); err == nil {
		return true
	}
	return false
}

func init() {
	rootCmd.AddCommand(downloadCommand(opts))
	rootCmd.AddCommand(listCommand(opts))
	rootCmd.AddCommand(moveCommand(opts))
	rootCmd.AddCommand(sendCommand(opts))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
