package worker

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"log"
	"sync"
)

// HandlerFunc is used to define the Handler that is run on for each message
type HandlerFunc func(msg types.Message) error

// HandleMessage wraps a function for handling sqs messages
func (f HandlerFunc) HandleMessage(msg types.Message) error {
	return f(msg)
}

// Handler interface
type Handler interface {
	HandleMessage(msg types.Message) error
}

// InvalidEventError struct
type InvalidEventError struct {
	event string
	msg   string
}

func (e InvalidEventError) Error() string {
	return fmt.Sprintf("[Invalid Event: %s] %s", e.event, e.msg)
}

// NewInvalidEventError creates InvalidEventError struct
func NewInvalidEventError(event, msg string) InvalidEventError {
	return InvalidEventError{event: event, msg: msg}
}

type SqsConsumeApi interface {
	GetQueueUrl(ctx context.Context,
		params *sqs.GetQueueUrlInput,
		optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)

	ReceiveMessage(ctx context.Context,
		params *sqs.ReceiveMessageInput,
		optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)

	DeleteMessage(ctx context.Context,
		params *sqs.DeleteMessageInput,
		optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// Worker struct
type Worker struct {
	Config    *Config
	Log       LoggerIFace
	SqsClient SqsConsumeApi
}

// Config struct
type Config struct {
	MaxNumberOfMessage int32
	QueueName          *string
	QueueURL           *string
	WaitTimeSecond     int32
}

// New sets up a new Worker
func New(client SqsConsumeApi, config *Config, logger LoggerIFace) *Worker {
	config.populateDefaultValues()
	if config.QueueURL == nil {
		queueURL, err := getQueueURL(client, config.QueueName)
		if err != nil {
			panic(err)
		}
		config.QueueURL = queueURL
	}

	return &Worker{
		Config:    config,
		Log:       logger,
		SqsClient: client,
	}
}

// Start starts the polling and will continue polling till the application is forcibly stopped
func (worker *Worker) Start(ctx context.Context, h Handler) {
	for {
		select {
		case <-ctx.Done():
			log.Println("worker: Stopping polling because a context kill signal was sent")
			return
		default:
			worker.Log.Debug("worker: Start Polling")

			params := &sqs.ReceiveMessageInput{
				QueueUrl:            worker.Config.QueueURL, // Required
				MaxNumberOfMessages: worker.Config.MaxNumberOfMessage,
				AttributeNames: []types.QueueAttributeName{
					types.QueueAttributeNameAll, // Required
				},
				MessageAttributeNames: []string{
					string(types.QueueAttributeNameAll),
				},
				WaitTimeSeconds: worker.Config.WaitTimeSecond,
			}

			resp, err := worker.SqsClient.ReceiveMessage(ctx, params)
			if err != nil {
				log.Println(err)
				continue
			}
			if len(resp.Messages) > 0 {
				worker.run(ctx, h, resp.Messages)
			}
		}
	}
}

// poll launches goroutine per received message and wait for all message to be processed
func (worker *Worker) run(ctx context.Context, h Handler, messages []types.Message) {
	numMessages := len(messages)
	worker.Log.Info(fmt.Sprintf("worker: Received %d messages", numMessages))

	var wg sync.WaitGroup
	wg.Add(numMessages)
	for i := range messages {
		go func(m types.Message) {
			// launch goroutine
			defer wg.Done()
			if err := worker.handleMessage(ctx, m, h); err != nil {
				worker.Log.Error(err.Error())
			}
		}(messages[i])
	}

	wg.Wait()
}

func (worker *Worker) handleMessage(ctx context.Context, m types.Message, h Handler) error {
	var err error
	err = h.HandleMessage(m)
	if _, ok := err.(InvalidEventError); ok {
		worker.Log.Error(err.Error())
	} else if err != nil {
		return err
	}

	params := &sqs.DeleteMessageInput{
		QueueUrl:      worker.Config.QueueURL, // Required
		ReceiptHandle: m.ReceiptHandle,        // Required
	}
	_, err = worker.SqsClient.DeleteMessage(ctx, params)
	if err != nil {
		return err
	}
	worker.Log.Debug(fmt.Sprintf("worker: deleted message from queue: %s", *m.ReceiptHandle))

	return nil
}
