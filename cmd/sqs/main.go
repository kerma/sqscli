package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	sqs "github.com/kerma/sqscli"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/spf13/cobra"
)

type Options struct {
	debug       bool
	profileName string
}

var opts = &Options{
	debug:       false,
	profileName: "default",
}

var rootCmd = &cobra.Command{
	Use:   "sqs",
	Short: "sqs is a CLI for AWS SQS",
}

func listCommand(opts *Options) *cobra.Command {
	var (
		urls bool
	)
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "list queues",
		RunE: func(cmd *cobra.Command, args []string) error {
			sess := session.Must(session.NewSession())
			client := sqs.New(sess)

			results, err := client.List()
			if err != nil {
				return err
			}

			if len(results) == 0 {
				fmt.Println("No queues")
				return nil
			}

			for _, url := range results {
				if urls {
					fmt.Println(url)
				} else {
					fmt.Println(url.Name())
				}
			}

			return nil
		},
	}
	cmd.Flags().BoolVarP(&urls, "urls", "u", false, "list as URLs")
	return cmd
}

func getCommand(opts *Options) *cobra.Command {
	var (
		all bool
	)
	cmd := &cobra.Command{
		Use:   "get [flags] [queueNames]",
		Short: "Get queue attributes",
		RunE: func(cmd *cobra.Command, args []string) error {
			if all == false && len(args) < 1 {
				return fmt.Errorf("At least one queue name is required")
			}
			sess := session.Must(session.NewSession())
			client := sqs.New(sess)

			var queueNames []string
			if all == true {
				list, err := client.List()
				if err != nil {
					return err
				}
				for _, val := range list {
					queueNames = append(queueNames, val.Name())
				}

			} else {
				queueNames = args
			}

			results, err := client.Get(queueNames)
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
	cmd.Flags().BoolVarP(&all, "all", "a", false, "Get attributes for all queues")
	return cmd
}

func init() {
	rootCmd.AddCommand(listCommand(opts))
	rootCmd.AddCommand(getCommand(opts))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
