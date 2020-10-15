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
	Status     string `json:"status"`
	Message    string `json:"message"`
	Timeout    int    `json:"timeout"`
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
