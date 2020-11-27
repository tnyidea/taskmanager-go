package taskmanager

import (
	"encoding/json"
)

type Task struct {
	// Primary Key
	Id int `json:"id"`

	// Task Reference Id
	ReferenceId string `json:"referenceId"`

	// Task Metadata
	TaskGroup  string `json:"taskGroup"`
	TaskType   string `json:"taskType"`
	Recurring  bool   `json:"recurring"`
	Status     string `json:"status"`
	Timeout    int    `json:"timeout"`
	Message    string `json:"message"`
	Properties []byte `json:"properties"`
}

func (t *Task) Bytes() []byte {
	b, _ := json.Marshal(t)
	return b
}

func (t *Task) String() string {
	b, _ := json.MarshalIndent(t, "", "    ")
	return string(b)
}
