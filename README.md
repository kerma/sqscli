# SQS CLI

```
Usage:
  sqs [command]

Available Commands:
  completion  generate the autocompletion script for the specified shell
  download    Download messages from queue
  help        Help about any command
  ls          list queues
  mv          Move messages from one queue to another
  send        Send a message to queue

Flags:
  -h, --help   help for sqs

Use "sqs [command] --help" for more information about a command.
```

## 

## Install

```
go install github.com/kerma/sqscli/cmd/sqs
```


## Why?

Because scripting awscli calls together is not so nice. This is much nicer:

```
$ sqs ls                                                   

NAME                     MESSAGES  IN-FLIGHT  TIMEOUT  MAX  DEAD-LETTER-TARGET
prod-incoming            10        4          120      1    prod-incoming_errors
stage-incoming           1         1          120      5    stage-incoming_errors
```

Moving messages from one queue to another is not directly possible using AWS Console or CLI.
`mv` helps here:

```
$ sqs mv prod-incoming_errors prod-incoming --limit 10
```
