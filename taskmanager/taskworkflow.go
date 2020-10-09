package taskmanager

import (
	"encoding/json"
	"log"
)

type TaskWorkflow struct {
	ContextProperties interface{}                      `json:"contextProperties"`
	TaskId            int                              `json:"taskId"`
	TaskProperties    []byte                           `json:"taskProperties"`
	Sequence          []string                         `json:"sequence"`
	Timeouts          map[string]int                   `json:"timeouts"`
	Handlers          map[string][]TaskWorkflowHandler `json:"handlers"`
}

type TaskWorkflowHandler func(w TaskWorkflow) error

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
		Handlers: map[string][]TaskWorkflowHandler{
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

func NextStatus(w TaskWorkflow) error {
	// NoOp Function to tell the workflow to increment task state in sequence
	return nil
}

func EndWorkflow(w TaskWorkflow) error {
	// NoOp Function to tell the workflow to end processing
	return nil
}

// TODO Add a Wait status to the task manager that must be part of the workflow
// TODO This Wait status must be part of the workflow -- if not it will be an error
func WaitForNotify(w TaskWorkflow) error {
	// NoOp Function to tell the workflow to do nothing and wait to resume or end
	return nil
}

func defaultCreateLogMessage(w TaskWorkflow) error {
	log.Println("Task Created: task", w.TaskId, "has been created")
	return nil
}

func defaultActiveLogMessage(w TaskWorkflow) error {
	log.Println("Task Active: task", w.TaskId, "is active")
	return nil
}

func defaultWaitingLogMessage(w TaskWorkflow) error {
	log.Println("Task Waiting: task", w.TaskId, "is waiting")
	return nil
}

func defaultCompleteLogMessage(w TaskWorkflow) error {
	log.Println("Task Complete: task", w.TaskId, "is complete")
	return nil
}

func defaultErrorLogMessage(w TaskWorkflow) error {
	log.Println("Task Error: task", w.TaskId, "has an error")
	return nil
}
