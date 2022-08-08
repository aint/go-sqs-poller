package worker

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockedSqsClient struct {
	Config   *aws.Config
	Response sqs.ReceiveMessageOutput
	SqsConsumeApi
	mock.Mock
}

func (c *mockedSqsClient) GetQueueUrl(_ context.Context, params *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	url := fmt.Sprintf("https://sqs.%v.amazonaws.com/123456789/%v", c.Config.Region, *params.QueueName)

	return &sqs.GetQueueUrlOutput{QueueUrl: &url}, nil
}

func (c *mockedSqsClient) ReceiveMessage(_ context.Context, params *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	c.Called(params)

	return &c.Response, nil
}

func (c *mockedSqsClient) DeleteMessage(_ context.Context, params *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	c.Called(params)
	c.Response = sqs.ReceiveMessageOutput{}

	return &sqs.DeleteMessageOutput{}, nil
}

type sqsEvent struct {
	Foo string `json:"foo"`
	Qux string `json:"qux"`
}

const maxNumberOfMessages = int32(1984)
const waitTimeSecond = int32(1337)

func TestStart(t *testing.T) {
	region := "eu-west-1"
	awsConfig := &aws.Config{Region: region}
	workerConfig := &Config{
		MaxNumberOfMessage: maxNumberOfMessages,
		QueueName:          aws.String("my-sqs-queue"),
		WaitTimeSecond:     waitTimeSecond,
	}

	clientParams := buildClientParams()
	sqsMessage := types.Message{Body: aws.String(`{ "foo": "bar", "qux": "baz" }`), ReceiptHandle: aws.String("h123")}
	sqsResponse := sqs.ReceiveMessageOutput{Messages: []types.Message{sqsMessage}}
	client := &mockedSqsClient{Response: sqsResponse, Config: awsConfig}
	deleteInput := &sqs.DeleteMessageInput{QueueUrl: clientParams.QueueUrl, ReceiptHandle: aws.String("h123")}

	worker := New(client, workerConfig, &Logger{})

	ctx, cancel := contextAndCancel()
	defer cancel()

	handlerFunc := HandlerFunc(func(msg types.Message) error { return nil })

	t.Run("the worker has correct configuration", func(t *testing.T) {
		assert.Equal(t, worker.Config.QueueName, aws.String("my-sqs-queue"), "QueueName has been set properly")
		assert.Equal(t, worker.Config.QueueURL, aws.String("https://sqs.eu-west-1.amazonaws.com/123456789/my-sqs-queue"), "QueueURL has been set properly")
		assert.Equal(t, worker.Config.MaxNumberOfMessage, maxNumberOfMessages, "MaxNumberOfMessage has been set properly")
		assert.Equal(t, worker.Config.WaitTimeSecond, waitTimeSecond, "WaitTimeSecond has been set properly")
	})

	t.Run("the worker has correct default configuration", func(t *testing.T) {
		minimumConfig := &Config{
			QueueName: aws.String("my-sqs-queue"),
		}
		worker := New(client, minimumConfig, &Logger{})

		assert.Equal(t, worker.Config.QueueName, aws.String("my-sqs-queue"), "QueueName has been set properly")
		assert.Equal(t, worker.Config.QueueURL, aws.String("https://sqs.eu-west-1.amazonaws.com/123456789/my-sqs-queue"), "QueueURL has been set properly")
		assert.Equal(t, worker.Config.MaxNumberOfMessage, int32(10), "MaxNumberOfMessage has been set by default")
		assert.Equal(t, worker.Config.WaitTimeSecond, int32(20), "WaitTimeSecond has been set by default")
	})

	t.Run("the worker successfully processes a message", func(t *testing.T) {
		client.On("ReceiveMessage", clientParams).Return()
		client.On("DeleteMessage", deleteInput).Return()

		worker.Start(ctx, handlerFunc)

		client.AssertExpectations(t)
	})
}

func contextAndCancel() (context.Context, context.CancelFunc) {
	delay := time.Now().Add(1 * time.Millisecond)

	return context.WithDeadline(context.Background(), delay)
}

func buildClientParams() *sqs.ReceiveMessageInput {
	url := aws.String("https://sqs.eu-west-1.amazonaws.com/123456789/my-sqs-queue")

	return &sqs.ReceiveMessageInput{
		QueueUrl:            url,
		MaxNumberOfMessages: maxNumberOfMessages,
		WaitTimeSeconds:     waitTimeSecond,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameAll,
		},
		MessageAttributeNames: []string{
			string(types.QueueAttributeNameAll),
		},
	}
}
