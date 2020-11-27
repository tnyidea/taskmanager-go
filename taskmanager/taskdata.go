package taskmanager

import (
	"database/sql"
	"strconv"
)

type sqlTask struct {
	// Primary Key
	Id sql.NullInt32 `sql:"id"`

	// Task Reference Id
	ReferenceId sql.NullString `sql:"reference_id"`

	// Task Metadata
	TaskGroup  sql.NullString `sql:"task_group"`
	TaskType   sql.NullString `sql:"task_type"`
	Recurring  sql.NullBool   `sql:"recurring"`
	Status     sql.NullString `sql:"status"`
	Timeout    sql.NullInt32  `sql:"timeout"`
	Message    sql.NullString `sql:"message"`
	Properties []byte         `sql:"properties"`
}

func (t *sqlTask) task() Task {
	task := Task{
		Id:          int(t.Id.Int32),
		ReferenceId: t.ReferenceId.String,
		TaskGroup:   t.TaskGroup.String,
		TaskType:    t.TaskType.String,
		Recurring:   t.Recurring.Bool,
		Status:      t.Status.String,
		Timeout:     int(t.Timeout.Int32),
		Message:     t.Message.String,
		Properties:  t.Properties,
	}

	return task
}

func rowSqlSourceTask(t Task) []interface{} {
	return []interface{}{
		t.ReferenceId,
		t.TaskGroup, t.TaskType,
		t.Recurring, t.Status, t.Timeout, t.Message,
		t.Properties,
	}
}

func (t *sqlTask) rowSqlDestination() []interface{} {
	return []interface{}{
		&t.Id, &t.ReferenceId,
		&t.TaskGroup, &t.TaskType,
		&t.Status, &t.Message, &t.Timeout,
		&t.Properties,
	}
}

const sqlTaskTable = "task_manager"
const sqlTaskColumns = `
    reference_id,
    task_group, task_type,
    recurring, status, timeout, message,
    properties`

const sqlCreateTask = `
    INSERT INTO ` + sqlTaskTable +
	` (` + sqlTaskColumns + `)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    RETURNING id`

const sqlUpdateTask = `
    UPDATE ` + sqlTaskTable + `
    SET (` + sqlTaskColumns + `) =
    ($2, $3, $4, $5, $6, $7, $8, $9)
    WHERE id = $1`

const sqlCountAllTasks = `
    SELECT count(id)
    FROM ` + sqlTaskTable

const sqlFindAllTasks = `
    SELECT id, ` + sqlTaskColumns + `
    FROM ` + sqlTaskTable

const sqlFindTask = sqlFindAllTasks +
	" WHERE id = $1"

const sqlFindAllTasksByGroupAndStatus = sqlFindAllTasks +
	" WHERE task_group = $1 AND status = $2"

const sqlFindAllTasksByTypeAndStatus = sqlFindAllTasks +
	" WHERE task_type = $1 AND status = $2"

const sqlFindAllRecurringTasks = sqlFindAllTasks +
	" WHERE recurring IS true"

const deleteTaskSQL = `
    DELETE FROM ` + sqlTaskTable + `
    WHERE id = $1`

func (m *TaskManager) CreateTask(t Task) (Task, error) {
	if t.Timeout < 1 {
		t.Timeout = -1
	}
	t.Status = "Created"

	var id int
	row := m.db.QueryRow(sqlCreateTask, rowSqlSourceTask(t)...)
	err := row.Scan(&id)
	if err != nil {
		return Task{}, err
	}

	t.Id = id
	return t, nil
}

func (m *TaskManager) CountAllTasks() (int, error) {
	row := m.db.QueryRow(sqlCountAllTasks)

	var count int
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (m *TaskManager) FindAllTasks(options map[string]string) ([]Task, error) {
	rows, err := m.db.Query(sqlFindAllTasks + findAllOptionsString(options))

	var result []Task
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t sqlTask
		err := rows.Scan(t.rowSqlDestination()...)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		result = append(result, t.task())
	}
	_ = rows.Close()

	return result, nil
}

func (m *TaskManager) FindTask(id int) (Task, error) {
	row := m.db.QueryRow(sqlFindTask, id)

	var t sqlTask
	err := row.Scan(t.rowSqlDestination()...)
	if err != nil {
		return Task{}, err
	}
	return t.task(), nil
}

func (m *TaskManager) FindAllTasksByGroupAndStatus(taskGroup string, status string, options map[string]string) ([]Task, error) {
	rows, err := m.db.Query(sqlFindAllTasksByGroupAndStatus+findAllOptionsString(options), taskGroup, status)

	var result []Task
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t sqlTask
		err := rows.Scan(t.rowSqlDestination()...)
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
	rows, err := m.db.Query(sqlFindAllTasksByTypeAndStatus+findAllOptionsString(options), taskType, status)

	var result []Task
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t sqlTask
		err := rows.Scan(t.rowSqlDestination()...)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		result = append(result, t.task())
	}
	_ = rows.Close()

	return result, nil
}

func (m *TaskManager) FindAllRecurringTasks(options map[string]string) ([]Task, error) {
	rows, err := m.db.Query(sqlFindAllRecurringTasks + findAllOptionsString(options))

	var result []Task
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t sqlTask
		err := rows.Scan(t.rowSqlDestination()...)
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

	_, err := m.db.Exec(sqlUpdateTask, append([]interface{}{t.Id}, rowSqlSourceTask(t)...)...)
	return err
}

func (m *TaskManager) DeleteTask(id int) error {
	_, err := m.db.Exec(deleteTaskSQL, id)
	return err
}

func findAllOptionsString(options map[string]string) string {
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
				return ""
			}
			rangeEnd, err := strconv.Atoi(options["rangeEnd"])
			if err != nil {
				return ""
			}

			limit := rangeEnd - rangeStart
			offset := rangeStart

			rangeSQL = " LIMIT " + strconv.Itoa(limit) + " OFFSET " + strconv.Itoa(offset) + " "
		}
	}

	return filterSQL + sortSQL + rangeSQL
}
