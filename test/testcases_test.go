package test

import (
	"context"
	"github.com/tnyidea/taskmanager-go/taskmanager"
	"log"
	"testing"
)

var testTaskManager taskmanager.TaskManager

var testTask = taskmanager.Task{
	Id:          0,
	ReferenceId: "ReferenceId",
	TaskGroup:   "TaskGroup",
	TaskType:    "TaskType",
	Status:      "Failed",
	Timeout:     300,
	Properties:  []byte(`{ "sampleData": "sampleData" }`),
}

var testContextProperties = make(map[string]string)

func TestDropTableTaskManager(t *testing.T) {
	err := dropTableTaskManager()
	if err != nil {
		log.Print("ignoring dropTableTaskManager error:", err)
	}
}

func TestCreateTableTaskManager(t *testing.T) {
	err := createTableTaskManager()
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
}

func TestInitializeTaskManager(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, taskmanager.ContextKey("testContextProperties"), testContextProperties)
	testTaskManager = taskmanager.New(ctx, TaskManagerTestDataUrl, map[string]taskmanager.TaskWorkflowDefinition{
		"TaskType": taskmanager.DefaultTaskWorkflow,
	})
}

func TestCreateAndStartTask(t *testing.T) {
	m := testTaskManager
	err := m.Open()
	if err != nil {
		log.Println("taskmanager.New:", err)
		t.FailNow()
	}
	defer m.Close()

	task, err := m.CreateTask(testTask)
	if err != nil {
		log.Println("taskmanager.CreateTask:", err)
		t.FailNow()
	}
	id := task.Id

	err = m.StartTask(id)
	if err != nil {
		log.Println("taskmanager.StartTask:", err)
		t.FailNow()
	}

	task, err = m.FindTask(id)
	if err != nil {
		log.Println("taskmanager.FindTask:", err)
		t.FailNow()
	}

	log.Println(&task)

	if task.Status != "Waiting" {
		log.Println("task with default workflow does not have 'Waiting' status after StartTask()")
		t.FailNow()
	}

	if task.Timeout != -1 {
		log.Println("task incremented to Waiting with default workflow does not have '-1' as timeout")
		t.FailNow()
	}

}

func TestUpdateTaskStatus(t *testing.T) {
	m := testTaskManager
	err := m.Open()
	if err != nil {
		log.Println("taskmanager.New:", err)
		t.FailNow()
	}
	defer m.Close()

	task, err := m.FindTask(3)
	if err != nil {
		log.Println("taskmanager.FindTask:", err)
		t.FailNow()
	}
	log.Println(&task)

	err = m.NotifyTaskWaitStatusResult(task.Id, "success")
	if err != nil {
		log.Println("taskmanager.UpdateTaskStatus:", err)
		t.FailNow()
	}

	task, err = m.FindTask(3)
	if err != nil {
		log.Println("taskmanager.FindTask:", err)
		t.FailNow()
	}

	log.Println(&task)

	if task.Status != "Complete" {
		log.Println("task with default workflow does not have 'Waiting' status after StartTask()")
		t.FailNow()
	}

	if task.Timeout != -1 {
		log.Println("task updated to Complete with default workflow does not have '-1' as timeout")
		t.FailNow()
	}
}
