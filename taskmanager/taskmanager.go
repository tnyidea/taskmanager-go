package taskmanager

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"reflect"
	"runtime"
	"strconv"
	"strings"
)

type TaskManager struct {
	Context       context.Context
	DatabaseTable string
	db            *sql.DB

	//	DataUrl string
	//	TaskTypeWorkflows map[string]TaskWorkflowDefinition

}

func (m *TaskManager) Bytes() []byte {
	b, _ := json.Marshal(m)
	return b
}

func (m *TaskManager) String() string {
	b, _ := json.MarshalIndent(m, "", "    ")
	return string(b)
}

type TaskWorkflowDefinition func(ctx context.Context) *TaskWorkflow

func New(ctx context.Context, dataUrl string, workflows map[string]TaskWorkflowDefinition) TaskManager {
	ctx = context.WithValue(ctx, ContextKey("taskManagerDataUrl"), dataUrl)
	ctx = context.WithValue(ctx, ContextKey("taskWorkflows"), workflows)
	return TaskManager{
		Context: ctx,
	}
}

func (m *TaskManager) Open() error {
	// Open Connection
	db, err := sql.Open("postgres", m.Context.Value(ContextKey("taskManagerDataUrl")).(string))
	if err != nil {
		return err
	}

	m.db = db
	return nil
}

func (m *TaskManager) Close() {
	// Close Connection but ignore any errors
	_ = m.db.Close()
}

func (m *TaskManager) ValidTaskType(t string) bool {
	workflows := m.Context.Value(ContextKey("taskWorkflows")).(map[string]TaskWorkflowDefinition)
	_, defined := workflows[t]
	return defined
}

func (m *TaskManager) StartTask(id int) error {
	task, err := m.FindTask(id)
	if err != nil {
		return errors.New("error starting task while finding task ID " + strconv.Itoa(id) + ": " + err.Error())
	}

	if !m.ValidTaskType(task.TaskType) {
		return errors.New("error starting task: invalid task type: " + task.TaskType)
	}

	// Create a Task Workflow Context
	ctx := m.Context
	ctx = context.WithValue(ctx, ContextKey("taskManager"), m)
	ctx = context.WithValue(ctx, ContextKey("task"), task)
	if task.Recurring {
		ctx = context.WithValue(ctx, ContextKey("recurringTask"), task)
	}

	workflows := m.Context.Value(ContextKey("taskWorkflows")).(map[string]TaskWorkflowDefinition)
	w := workflows[task.TaskType](ctx)

	if task.Status != "Created" {
		errMessage := "invalid task state for StartTask(): " + task.Status +
			".  Task must be in created state before startng"
		m.handleTaskError(task, w, errMessage)
		return errors.New(errMessage)
	}

	statusHandlers := w.Handlers["Created"]
	log.Println(statusHandlers)
	for i := range statusHandlers {
		handlerName := runtime.FuncForPC(reflect.ValueOf(statusHandlers[i]).Pointer()).Name()
		log.Println(handlerName)
		if strings.HasSuffix(handlerName, "NextStatus") {
			err := m.incrementTaskStatus(task, w)
			if err != nil {
				log.Println(err)
				return err
			}
			break
		}
		err := statusHandlers[i](w)
		if err != nil {
			errMessage := "error executing handlers for " + strconv.Itoa(i) + " with task " + strconv.Itoa(task.Id)
			m.handleTaskError(task, w, err.Error())
			return errors.New(errMessage)
		}
	}

	return nil
}

func (m *TaskManager) NotifyTaskWaitStatusResult(id int, result string, message string) error {
	t, err := m.FindTask(id)
	if err != nil {
		errMessage := fmt.Sprintf("error finding task ID %d.  Task must be created before executing workflow", id)
		return errors.New(errMessage)
	}

	// Create a Task Workflow Context
	ctx := m.Context
	ctx = context.WithValue(ctx, ContextKey("taskManager"), m)
	ctx = context.WithValue(ctx, ContextKey("task"), t)

	workflows := m.Context.Value(ContextKey("taskWorkflows")).(map[string]TaskWorkflowDefinition)
	w := workflows[t.TaskType](ctx)

	switch result {
	case "success":
		return m.incrementTaskStatus(t, w)
	case "error":
		m.handleTaskError(t, w, message)
		return nil
	default:
		errMessage := "invalid result type " + result
		log.Println(errMessage)
		return errors.New(errMessage)
	}
}

func (m *TaskManager) incrementTaskStatus(t Task, w *TaskWorkflow) error {
	log.Println("Incrementing Task Status for task", t.Id)
	_, err := m.FindTask(t.Id)
	if err != nil {
		errMessage := "error finding task ID " + strconv.Itoa(t.Id) + ".  Task must be created before executing workflow"
		m.handleTaskError(t, w, err.Error())
		return errors.New(errMessage)
	}

	log.Println("Current Task Status is :", t.Status)

	// If task status is last in sequence then somehow we got here in error
	if w.Sequence[len(w.Sequence)-1] == t.Status {
		errMessage := "invalid task workflow definition: EndWorkflow function expected after '" +
			t.Status + "' handler execution"
		m.handleTaskError(t, w, errMessage)
		return errors.New(errMessage)
	}

	log.Println("Current status is not last status")
	// Otherwise we are not on the last status, so process
	for i := range w.Sequence {
		if w.Sequence[i] == t.Status {
			status := t.Status
			nextStatus := w.Sequence[i+1]
			log.Println("Found next status of:", nextStatus)

			t.Status = nextStatus
			t.Timeout = w.Timeouts[nextStatus]

			// Update the task manager with the cached task properties
			t.Properties = w.GetTask().Properties

			err = m.UpdateTask(t)
			if err != nil {
				errMessage := "error updating task ID " + strconv.Itoa(t.Id) + " with status '" + status + "' to new status '" + nextStatus + "'"
				m.handleTaskError(t, w, err.Error())
				return errors.New(errMessage)
			}

			// Update the cached version of the task
			w.UpdateTask(t)

			statusHandlers := w.Handlers[nextStatus]
			log.Println("executing next statusHandlers...", statusHandlers)
			for j := range statusHandlers {
				handlerName := runtime.FuncForPC(reflect.ValueOf(statusHandlers[j]).Pointer()).Name()
				if strings.HasSuffix(handlerName, "EndWorkflow") {
					// Reset the task if it is a recurring task
					if t.Recurring {
						resetRecurringTask(t, w)
					}
					break
				}
				if strings.HasSuffix(handlerName, "NextStatus") {
					err := m.incrementTaskStatus(t, w)
					if err != nil {
						return err
					}
					break
				}
				err := statusHandlers[j](w)
				if err != nil {
					errMessage := "error executing handlers for status '" + nextStatus +
						"' with task ID " + strconv.Itoa(t.Id)
					t.Message = err.Error()
					m.handleTaskError(t, w, err.Error())
					return errors.New(errMessage)
				}
			}
			break
		}
	}

	return nil
}

func (m *TaskManager) handleTaskError(t Task, w *TaskWorkflow, message string) {
	log.Println("Handling Task ", t.Id, "Error:", message)
	t.Status = "Error"
	t.Message = message

	err := m.UpdateTask(t) // No need to handle error from Update (other than log it) since we are already here
	if err != nil {
		log.Println(err)
	}

	errorHandlers := w.Handlers["Error"]
	for i := range errorHandlers {
		_ = errorHandlers[i](w)
	}

	// Reset the task if it is a recurring task
	if t.Recurring {
		resetRecurringTask(t, w)
	}
}

func resetRecurringTask(t Task, w *TaskWorkflow) {
	m := w.GetTaskManager()
	recurringTask := w.Context.Value(ContextKey("recurringTask")).(Task)
	_, err := m.CreateTask(recurringTask)
	if err != nil {
		log.Println("Warning: could not reset recurring task "+
			strconv.Itoa(t.Id)+":", err)
	}
}
