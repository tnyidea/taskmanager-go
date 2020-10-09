package taskmanager

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"strings"
)

type TaskManager struct {
	DataUrl string `json:"dataUrl"`
	db      *sql.DB
}

func (m *TaskManager) String() string {
	byteValue, _ := json.MarshalIndent(m, "", "    ")
	return string(byteValue)
}

func New(dataUrl string) (TaskManager, error) {
	// Open Connection
	db, err := sql.Open("postgres", dataUrl)
	if err != nil {
		log.Println(err)
		return TaskManager{}, err
	}

	return TaskManager{
		DataUrl: dataUrl,
		db:      db,
	}, nil
}

func (m *TaskManager) Close() {
	_ = m.db.Close()
}

func (m *TaskManager) StartTask(id int, w TaskWorkflow) error {
	if w.ContextProperties == nil {
		errMessage := "workflow context properties is undefined"
		return errors.New(errMessage)
	}
	task, err := m.FindTask(id)
	if err != nil {
		errMessage := fmt.Sprintf("error finding task ID %d.  Task must be created before starting", id)
		return errors.New(errMessage)
	}

	if task.Status != "Created" {
		errMessage := fmt.Sprintf("invalid task state for StartTask(): %v.  Task must be in created state before startng", task.Status)
		m.handleTaskError(task, w, errMessage)
		return errors.New(errMessage)
	}

	// Cache the Task Information in the workflow
	w.TaskId = task.Id
	w.TaskProperties = task.Properties

	statusHandlers := w.Handlers["Created"]
	for i := range statusHandlers {
		handlerName := runtime.FuncForPC(reflect.ValueOf(statusHandlers[i]).Pointer()).Name()
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
			errMessage := fmt.Sprintf("error executing handlers for %v with task %d", i, task.Id)
			m.handleTaskError(task, w, errMessage)
			return errors.New(errMessage)
		}
	}

	return nil
}

func (m *TaskManager) NotifyTaskWaitStatusResult(id int, result string, w TaskWorkflow) error {
	t, err := m.FindTask(id)
	if err != nil {
		errMessage := fmt.Sprintf("error finding task ID %d.  Task must be created before executing workflow", id)
		return errors.New(errMessage)
	}

	switch result {
	case "success":
		return m.incrementTaskStatus(t, w)
	case "error":
		// TODO get messaging as part of this
		m.handleTaskError(t, w, "Error")
		return nil
	default:
		errMessage := fmt.Sprintf("invalid result type %v", result)
		log.Println(errMessage)
		return errors.New(errMessage)
	}
}

func (m *TaskManager) incrementTaskStatus(t Task, w TaskWorkflow) error {
	_, err := m.FindTask(t.Id)
	if err != nil {
		errMessage := fmt.Sprintf("error finding task ID %d.  Task must be created before executing workflow", t.Id)
		m.handleTaskError(t, w, errMessage)
		return errors.New(errMessage)
	}

	for i := range w.Sequence {
		if w.Sequence[i] == t.Status {
			status := t.Status
			nextStatus := w.Sequence[i+1]

			t.Status = nextStatus
			t.Timeout = w.Timeouts[nextStatus]

			err = m.UpdateTask(t)
			if err != nil {
				errMessage := fmt.Sprintf("error updating task ID %d with status '%v' to new status '%v'", t.Id, status, nextStatus)
				m.handleTaskError(t, w, errMessage)
				return errors.New(errMessage)
			}

			statusHandlers := w.Handlers[nextStatus]
			for j := range statusHandlers {
				handlerName := runtime.FuncForPC(reflect.ValueOf(statusHandlers[j]).Pointer()).Name()
				if strings.HasSuffix(handlerName, "EndWorkflow") {
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
					errMessage := fmt.Sprintf("error executing handlers for status '%v' with task ID %d", nextStatus, t.Id)
					t.Message = err.Error()
					m.handleTaskError(t, w, errMessage)
					return errors.New(errMessage)
				}
			}
			break
		}
	}

	return nil
}

func (m *TaskManager) handleTaskError(t Task, w TaskWorkflow, message string) {
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
}
