package test

import (
	"database/sql"
	"github.com/tnyidea/taskmanager-go/taskmanager"
	"log"
	"testing"
)

var testTaskManager = taskmanager.New(TaskManagerTestDataUrl,
	map[string]taskmanager.TaskWorkflowDefinition{
		"TaskType": taskmanager.DefaultTaskWorkflow,
	})

var testTask = taskmanager.Task{
	Id:          0,
	ReferenceId: "ReferenceId",
	TaskType:    "TaskType",
	Status:      "Failed",
	Timeout:     300,
	Properties:  []byte(`{ "sampleData": "sampleData" }`),
}

func TestDropTableTaskManager(t *testing.T) {
	err := dropTableTaskManagerBlue()
	if err != nil {
		log.Print("ignoring DropTableTaskManagerBlue error:", err)
	}
}

func TestCreateTableTaskManager(t *testing.T) {
	err := createTableTaskManagerBlue()
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
}

func TestCreateAndFindTask(t *testing.T) {
	m := testTaskManager
	err := m.Open()
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
	defer m.Close()

	createFindTask := testTask

	task, err := m.CreateTask(createFindTask)
	if err != nil {
		log.Println("CreateTask", err)
		t.FailNow()
	}
	id := task.Id

	task, err = m.FindTask(id)
	if err != nil {
		log.Println("FindTask", err)
		t.FailNow()
	}

	want := testTask
	want.Id = id
	want.Status = "Created"

	log.Println(&want)
	log.Println(&task)

	if task.String() != want.String() {
		log.Println("expected FindTask result does not match testTask")
		t.FailNow()
	}
}

func TestFindAllTasks(t *testing.T) {
	m := testTaskManager
	err := m.Open()
	if err != nil {
		log.Println(err)
		m.Close()
		t.FailNow()
	}

	tasks, err := m.FindAllTasks(nil)
	if err != nil {
		log.Println(err)
		m.Close()
		t.FailNow()
	}
	m.Close()

	count := len(tasks)
	want := count != 0
	if !want {
		log.Println("expected FindAllTasks Count to be != 0: result received:", count)
		m.Close()
		t.FailNow()
	}
}

func TestUpdateTask(t *testing.T) {
	m := testTaskManager
	err := m.Open()
	if err != nil {
		log.Println(err)
		m.Close()
		t.FailNow()
	}

	want := testTask
	want.Id = 1
	want.TaskType = "DifferentType"

	err = m.UpdateTask(want)
	if err != nil {
		log.Println(err)
		m.Close()
		t.FailNow()
	}

	task, err := m.FindTask(want.Id)
	if err != nil {
		log.Println(err)
		m.Close()
		t.FailNow()
	}
	m.Close()

	log.Println(&task)
	log.Println(&want)

	if task.String() != want.String() {
		log.Println("expected FindTask result does not match test Task")
		m.Close()
		t.FailNow()
	}
}

func TestDeleteTask(t *testing.T) {
	m := testTaskManager
	err := m.Open()
	if err != nil {
		log.Println(err)
		m.Close()
		t.FailNow()
	}

	id := 1
	err = m.DeleteTask(id)
	if err != nil {
		log.Println(err)
		m.Close()
		t.FailNow()
	}

	_, err = m.FindTask(id)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Println(err)
			m.Close()
			t.FailNow()
		}
	}
	m.Close()
}

func TestCreateAndFindNullTask(t *testing.T) {
	m := testTaskManager
	err := m.Open()
	if err != nil {
		log.Println(err)
		m.Close()
		t.FailNow()
	}

	nullTask := taskmanager.Task{
		TaskType:   "NullTask",
		Properties: []byte("{}"),
	}

	task, err := m.CreateTask(nullTask)
	if err != nil {
		log.Println("CreateTask:", err)
		m.Close()
		t.FailNow()
	}
	id := task.Id

	task, err = m.FindTask(id)
	if err != nil {
		log.Println("FindTask:", err)
		m.Close()
		t.FailNow()
	}

	want := nullTask
	want.Id = id
	want.Status = "Created"
	want.Timeout = -1

	if task.String() != want.String() {
		log.Println("expected FindTask result does not match nullTask")
		m.Close()
		t.FailNow()
	}

	err = m.DeleteTask(id)
	if err != nil {
		log.Println("DeleteTask:", err)
		m.Close()
		t.FailNow()
	}
	m.Close()
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

	err = m.StartTask(id, m.TaskTypeWorkflows[task.TaskType](nil))
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

	err = m.NotifyTaskWaitStatusResult(task.Id, "success", m.TaskTypeWorkflows[task.TaskType](nil))
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
