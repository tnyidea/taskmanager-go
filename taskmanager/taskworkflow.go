package taskmanager

import (
	"encoding/json"
	"log"
)

type TaskWorkflow struct {
	ContextProperties interface{} `json:"contextProperties"`
	Cache             struct {
		TaskId         int         `json:"taskId"`
		TaskProperties interface{} `json:"taskProperties"`
	} `json:"cache"`
	Sequence []string                                                `json:"sequence"`
	Timeouts map[string]int                                          `json:"timeouts"`
	Handlers map[string][]func(id int, properties interface{}) error `json:"handlers"`
}

func (w *TaskWorkflow) Bytes() []byte {
	b, _ := json.Marshal(w)
	return b
}

func (w *TaskWorkflow) String() string {
	b, _ := json.MarshalIndent(w, "", "    ")
	return string(b)
}

func DefaultTaskWorkflow() TaskWorkflow {
	return TaskWorkflow{
		ContextProperties: make(map[string]string),
		Sequence: []string{
			"Created", "Active", "Waiting", "Complete",
		},
		Timeouts: map[string]int{
			"Created": -1, "Active": 300, "Waiting": -1, "Complete": -1, "Error": -1, "Timeout": -1,
		},
		Handlers: map[string][]func(id int, properties interface{}) error{
			"Created": {
				defaultCreateLogMessage,
				NextStatus,
			},
			"Active": {
				defaultActiveLogMessage,
				NextStatus,
			},
			"Waiting": {
				defaultWaitingLogMessage,
				WaitForNotify,
			},
			"Complete": {
				defaultCompleteLogMessage,
				EndWorkflow,
			},
			"Error": {
				defaultErrorLogMessage,
			},
		},
	}
}

func (w *TaskWorkflow) CacheTaskProperties(id int, properties interface{}) {
	w.Cache.TaskId = id
	w.Cache.TaskProperties = properties
}

func NextStatus(id int, properties interface{}) error {
	// NoOp Function to tell the workflow to increment task state in sequence
	return nil
}

func EndWorkflow(id int, properties interface{}) error {
	// NoOp Function to tell the workflow to end processing
	return nil
}

// TODO Add a Wait status to the task manager that must be part of the workflow
// TODO This Wait status must be part of the workflow -- if not it will be an error
func WaitForNotify(id int, properties interface{}) error {
	// NoOp Function to tell the workflow to do nothing and wait to resume or end
	return nil
}

func defaultCreateLogMessage(id int, properties interface{}) error {
	log.Println("Task Created: task", id, "has been created")
	return nil
}

func defaultActiveLogMessage(id int, properties interface{}) error {
	log.Println("Task Active: task", id, "is active")
	return nil
}

func defaultWaitingLogMessage(id int, properties interface{}) error {
	log.Println("Task Waiting: task", id, "is waiting")
	return nil
}

func defaultCompleteLogMessage(id int, properties interface{}) error {
	log.Println("Task Complete: task", id, "is complete")
	return nil
}

func defaultErrorLogMessage(id int, properties interface{}) error {
	log.Println("Task Error: task", id, "has an error")
	return nil
}
