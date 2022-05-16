package worker

import (
	"fmt"
	"log"
)

// LoggerIFace interface
type LoggerIFace interface {
	Debug(i ...interface{})
	Info(i ...interface{})
	Error(i ...interface{})
}

type Logger struct {
}

func (l *Logger) Debug(i ...interface{}) {
	log.Printf("[DEBUG] %s", fmt.Sprintln(i...))
}

func (l *Logger) Info(i ...interface{}) {
	log.Printf("[INFO] %s", fmt.Sprintln(i...))
}

func (l *Logger) Error(i ...interface{}) {
	log.Printf("[ERROR] %s", fmt.Sprintln(i...))
}
