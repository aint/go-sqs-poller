package main

import (
	"context"
	"fmt"
	"github.com/aint/go-sqs-poller/worker"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
	if err != nil {
		panic(err)
	}

	sqsClient := worker.CreateSqsClient(cfg)
	workerConfig := &worker.Config{
		QueueName:          aws.String("dev-notifications-sender-v2"), //my-sqs-queue
		MaxNumberOfMessage: 10,
		WaitTimeSecond:     5,
	}
	eventWorker := worker.New(sqsClient, workerConfig, &worker.Logger{})
	ctx := context.Background()

	// start the worker
	eventWorker.Start(ctx, func(msg types.Message) error {
		fmt.Println(aws.ToString(msg.Body))
		return nil
	})
}
