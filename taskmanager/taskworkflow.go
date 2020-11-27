package taskmanager

import (
	"context"
	"encoding/json"
	"log"
)

type TaskWorkflow struct {
	Context  context.Context                  `json:"context"`
	Sequence []string                         `json:"sequence"`
	Timeouts map[string]int                   `json:"timeouts"`
	Handlers map[string][]TaskWorkflowHandler `json:"handlers"`
}

type ContextKey string

type TaskWorkflowHandler func(w *TaskWorkflow) error

func (w *TaskWorkflow) Bytes() []byte {
	b, _ := json.Marshal(w)
	return b
}

func (w *TaskWorkflow) String() string {
	b, _ := json.MarshalIndent(w, "", "    ")
	return string(b)
}

func (w *TaskWorkflow) GetTaskManager() *TaskManager {
	return w.Context.Value(ContextKey("taskManager")).(*TaskManager)
}

func (w *TaskWorkflow) GetTask() *Task {
	return w.Context.Value(ContextKey("task")).(*Task)
}

func (w *TaskWorkflow) UpdateTask(t *Task) {
	ctx := w.Context
	ctx = context.WithValue(ctx, ContextKey("task"), t)
	w.Context = ctx
}

func DefaultTaskWorkflow(ctx context.Context) *TaskWorkflow {
	return &TaskWorkflow{
		Context: ctx,
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

func NextStatus(w *TaskWorkflow) error {
	// NoOp Function to tell the workflow to increment task state in sequence
	return nil
}

func EndWorkflow(w *TaskWorkflow) error {
	// NoOp Function to tell the workflow to end processing
	return nil
}

func WaitForNotify(w *TaskWorkflow) error {
	// NoOp Function to tell the workflow to do nothing and wait to resume or end
	return nil
}

func defaultCreateLogMessage(w *TaskWorkflow) error {
	log.Println("Task Created: task", w.GetTask().Id, "has been created")
	return nil
}

func defaultActiveLogMessage(w *TaskWorkflow) error {
	log.Println("Task Active: task", w.GetTask().Id, "is active")
	return nil
}

func defaultWaitingLogMessage(w *TaskWorkflow) error {
	log.Println("Task Waiting: task", w.GetTask().Id, "is waiting")
	return nil
}

func defaultCompleteLogMessage(w *TaskWorkflow) error {
	log.Println("Task Complete: task", w.GetTask().Id, "is complete")
	return nil
}

func defaultErrorLogMessage(w *TaskWorkflow) error {
	log.Println("Task Error: task", w.GetTask().Id, "has an error")
	return nil
}
