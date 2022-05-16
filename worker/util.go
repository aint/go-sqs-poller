package worker

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// CreateSqsClient creates a client for SQS API
func CreateSqsClient(awsConfig aws.Config) SqsConsumeApi {
	return sqs.NewFromConfig(awsConfig)
}

func (config *Config) populateDefaultValues() {
	if config.MaxNumberOfMessage == 0 {
		config.MaxNumberOfMessage = 10
	}

	if config.WaitTimeSecond == 0 {
		config.WaitTimeSecond = 20
	}
}

func getQueueURL(client SqsConsumeApi, queueName *string) (*string, error) {
	getQueueInput := &sqs.GetQueueUrlInput{
		QueueName: queueName,
	}

	queueOutput, err := client.GetQueueUrl(context.Background(), getQueueInput)
	if err != nil {
		return nil, err
	}

	return queueOutput.QueueUrl, nil
}
