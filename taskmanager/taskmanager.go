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

	//if task.Recurring {
	//	if time.Now().Before(task.CreatedAt.Add(time.Duration(task.Timeout)*time.Second)) {
	//		log.Println("cannot start task: recurring task " +strconv.Itoa(task.Id) + " has not timed out for next execution")
	//		return nil
	//	}
	//}

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
		m.handleTaskError(w, errMessage)
		return errors.New(errMessage)
	}

	statusHandlers := w.Handlers["Created"]
	for i := range statusHandlers {
		handlerName := runtime.FuncForPC(reflect.ValueOf(statusHandlers[i]).Pointer()).Name()
		if strings.HasSuffix(handlerName, "NextStatus") {
			err := m.incrementTaskStatus(w)
			if err != nil {
				log.Println(err)
				return err
			}
			break
		}
		err := statusHandlers[i](w)
		if err != nil {
			errMessage := "error executing handlers for " + strconv.Itoa(i) + " with task " + strconv.Itoa(task.Id)
			m.handleTaskError(w, err.Error())
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
		return m.incrementTaskStatus(w)
	case "error":
		m.handleTaskError(w, message)
		return nil
	default:
		errMessage := "invalid result type " + result
		log.Println(errMessage)
		return errors.New(errMessage)
	}
}

func (m *TaskManager) incrementTaskStatus(w *TaskWorkflow) error {
	task := w.GetTask()

	// If task status is last in sequence then somehow we got here in error
	if w.Sequence[len(w.Sequence)-1] == task.Status {
		errMessage := "invalid task workflow definition: EndWorkflow function expected after '" +
			task.Status + "' handler execution"
		m.handleTaskError(w, errMessage)
		return errors.New(errMessage)
	}

	// Otherwise we are not on the last status, so process
	for i := range w.Sequence {
		if w.Sequence[i] == task.Status {
			status := task.Status
			nextStatus := w.Sequence[i+1]

			// Update the Task State
			task.Status = nextStatus
			task.Timeout = w.Timeouts[nextStatus]

			//  - update the cached version of the task
			w.UpdateTask(task)

			//  - update the database version of the task
			err := m.UpdateTask(task)
			if err != nil {
				errMessage := "error updating task ID " + strconv.Itoa(task.Id) + " with status '" + status + "' to new status '" + nextStatus + "'"
				m.handleTaskError(w, err.Error())
				return errors.New(errMessage)
			}

			// Call the nextStatus Handlers
			statusHandlers := w.Handlers[nextStatus]
			for j := range statusHandlers {
				handlerName := runtime.FuncForPC(reflect.ValueOf(statusHandlers[j]).Pointer()).Name()
				if strings.HasSuffix(handlerName, "EndWorkflow") {
					// Reset the task if it is a recurring task
					if task.Recurring {
						resetRecurringTask(w)
					}
					break
				}
				if strings.HasSuffix(handlerName, "NextStatus") {
					err := m.incrementTaskStatus(w)
					if err != nil {
						return err
					}
					break
				}
				err := statusHandlers[j](w)
				if err != nil {
					errMessage := "error executing handlers for status '" + nextStatus +
						"' with task ID " + strconv.Itoa(task.Id)
					task.Message = err.Error()
					m.handleTaskError(w, err.Error())
					return errors.New(errMessage)
				}
			}
			break
		}
	}

	return nil
}

func (m *TaskManager) handleTaskError(w *TaskWorkflow, message string) {
	task := w.GetTask()

	// Update the Task State
	task.Status = "Error"
	task.Message = message

	//  - update the cached version of the task
	w.UpdateTask(task)

	//  - update the database version of the task
	err := m.UpdateTask(task) // No need to handle error from Update (other than log it) since we are already here
	if err != nil {
		log.Println(err)
	}

	errorHandlers := w.Handlers["Error"]
	for i := range errorHandlers {
		_ = errorHandlers[i](w)
	}

	// Reset the task if it is a recurring task
	if task.Recurring {
		resetRecurringTask(w)
	}
}

func resetRecurringTask(w *TaskWorkflow) {
	task := w.GetTask()

	// Update recurring task with cached reference Id
	recurringTask := w.Context.Value(ContextKey("recurringTask")).(Task)
	recurringTask.ReferenceId = task.ReferenceId

	// Create next recurring task
	m := w.GetTaskManager()
	_, err := m.CreateTask(recurringTask)
	if err != nil {
		log.Println("Warning: could not reset recurring task "+
			strconv.Itoa(task.Id)+":", err)
	}
}
