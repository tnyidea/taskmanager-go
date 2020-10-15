package taskmanager

import (
	"database/sql"
	"strconv"
)

type pqTaskManager struct {
	// Primary Key
	Id sql.NullInt32 `sql:"id"`

	// Task Reference Id
	ReferenceId sql.NullString `sql:"reference_id"`

	// Task Metadata
	TaskGroup  sql.NullString `sql:"task_group"`
	TaskType   sql.NullString `sql:"task_type"`
	Status     sql.NullString `sql:"status"`
	Timeout    sql.NullInt32  `sql:"timeout"`
	Properties []byte         `sql:"properties"`
}

func (t *pqTaskManager) task() Task {
	task := Task{
		Id:          int(t.Id.Int32),
		ReferenceId: t.ReferenceId.String,
		TaskGroup:   t.TaskGroup.String,
		TaskType:    t.TaskType.String,
		Status:      t.Status.String,
		Timeout:     int(t.Timeout.Int32),
		Properties:  t.Properties,
	}

	return task
}

func rowSourceTask(t Task) []interface{} {
	return []interface{}{
		t.ReferenceId,
		t.TaskGroup, t.TaskType, t.Status, t.Timeout,
		t.Properties,
	}
}

func (t *pqTaskManager) rowDestination() []interface{} {
	return []interface{}{
		&t.Id, &t.ReferenceId,
		&t.TaskGroup, &t.TaskType, &t.Status, &t.Timeout,
		&t.Properties,
	}
}

const columnsTaskManager = `
    reference_id,
    task_group, task_type, status, timeout,
    properties`

const createTaskSQL = `
    INSERT INTO task_manager (` + columnsTaskManager + `)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING id`

const updateTaskSQL = `
    UPDATE task_manager
    SET (` + columnsTaskManager + `) =
    ($2, $3, $4, $5, $6, $7)
    WHERE id = $1`

const countTasksSQL = `
    SELECT count(id)
    FROM task_manager`

const findTaskSQL = `
    SELECT id, ` + columnsTaskManager + `
    FROM task_manager
    WHERE id = $1`

const findAllTasksSQL = `
    SELECT id, ` + columnsTaskManager + `
    FROM task_manager`

const findAllTasksByGroupAndStatusSQL = `
    SELECT id, ` + columnsTaskManager + `
    FROM task_manager
    WHERE task_group = $1 AND status = $2`

const findAllTasksByTypeAndStatusSQL = `
    SELECT id, ` + columnsTaskManager + `
    FROM task_manager
    WHERE task_type = $1 AND status = $2`

const deleteTaskSQL = `
    DELETE FROM task_manager
    WHERE id = $1`

func (m *TaskManager) CreateTask(t Task) (Task, error) {
	if t.Timeout < 1 {
		t.Timeout = -1
	}
	t.Status = "Created"

	var id int
	row := m.db.QueryRow(createTaskSQL, rowSourceTask(t)...)
	err := row.Scan(&id)
	if err != nil {
		return Task{}, err
	}

	t.Id = id
	return t, nil
}

func (m *TaskManager) CountAllTasks() (int, error) {
	row := m.db.QueryRow(countTasksSQL)

	var count int
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (m *TaskManager) FindTask(id int) (Task, error) {
	row := m.db.QueryRow(findTaskSQL, id)

	var t pqTaskManager
	err := row.Scan(t.rowDestination()...)
	if err != nil {
		return Task{}, err
	}
	return t.task(), nil
}

func (m *TaskManager) FindAllTasks(options map[string]string) ([]Task, error) {
	queryOptions, err := findAllOptionsString(options)
	rows, err := m.db.Query(findAllTasksSQL + queryOptions)

	var result []Task
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t pqTaskManager
		err := rows.Scan(t.rowDestination()...)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		result = append(result, t.task())
	}
	_ = rows.Close()

	return result, nil
}

func (m *TaskManager) FindAllTasksByGroupAndStatus(taskGroup string, status string, options map[string]string) ([]Task, error) {
	queryOptions, err := findAllOptionsString(options)
	rows, err := m.db.Query(findAllTasksByGroupAndStatusSQL+queryOptions, taskGroup, status)

	var result []Task
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t pqTaskManager
		err := rows.Scan(t.rowDestination()...)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		result = append(result, t.task())
	}
	_ = rows.Close()

	return result, nil
}

func (m *TaskManager) FindAllTasksByTypeAndStatus(taskType string, status string, options map[string]string) ([]Task, error) {
	queryOptions, err := findAllOptionsString(options)
	rows, err := m.db.Query(findAllTasksByTypeAndStatusSQL+queryOptions, taskType, status)

	var result []Task
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t pqTaskManager
		err := rows.Scan(t.rowDestination()...)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		result = append(result, t.task())
	}
	_ = rows.Close()

	return result, nil
}

func (m *TaskManager) UpdateTask(t Task) error {
	if t.Timeout < 1 {
		t.Timeout = -1
	}

	_, err := m.db.Exec(updateTaskSQL, append([]interface{}{t.Id}, rowSourceTask(t)...)...)
	return err
}

func (m *TaskManager) DeleteTask(id int) error {
	_, err := m.db.Exec(deleteTaskSQL, id)
	return err
}

func findAllOptionsString(options map[string]string) (string, error) {
	// TODO This is the best way to do indexes on maps
	defined := make(map[string]bool)
	for i := range options {
		defined[i] = options[i] != ""
	}

	filterSQL := " "
	sortSQL := " "
	rangeSQL := " "

	if options != nil {
		if defined["filterColumn"] && defined["filterValue"] {
			filterSQL = " WHERE " + options["filterColumn"] + " ILIKE '" + options["filterValue"] + "%' "
		}
		if defined["sortColumn"] && defined["sortOrder"] {
			sortSQL = " ORDER BY " + options["sortColumn"] + " " + options["sortOrder"] + " "
		}
		if defined["rangeStart"] && defined["rangeEnd"] {
			rangeStart, err := strconv.Atoi(options["rangeStart"])
			if err != nil {
				return "", err
			}
			rangeEnd, err := strconv.Atoi(options["rangeEnd"])
			if err != nil {
				return "", err
			}

			limit := rangeEnd - rangeStart
			offset := rangeStart

			rangeSQL = " LIMIT " + strconv.Itoa(limit) + " OFFSET " + strconv.Itoa(offset) + " "
		}
	}

	return filterSQL + sortSQL + rangeSQL, nil
}
