package taskmanager

import (
	"database/sql"
	_ "github.com/lib/pq"
)

type pqTaskManagerBlue struct {
	// Primary Key
	Id sql.NullInt32 `sql:"id"`

	// Task Reference Id
	ReferenceId sql.NullString `sql:"reference_id"`

	// Task Metadata
	TaskType   sql.NullString `sql:"task_type"`
	Status     sql.NullString `sql:"status"`
	Timeout    sql.NullInt32  `sql:"timeout"`
	Properties []byte         `sql:"properties"`
}

func (t *pqTaskManagerBlue) task() Task {
	task := Task{
		Id:          int(t.Id.Int32),
		ReferenceId: t.ReferenceId.String,
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
		t.TaskType, t.Status, t.Timeout,
		t.Properties,
	}
}

func (t *pqTaskManagerBlue) rowDestination() []interface{} {
	return []interface{}{
		&t.Id, &t.ReferenceId,
		&t.TaskType, &t.Status, &t.Timeout,
		&t.Properties,
	}
}

const columnsTaskManagerBlue = `
    reference_id,
    task_type, status, timeout,
    properties`

const createTaskBlueSQL = `
    INSERT INTO task_manager_blue (` + columnsTaskManagerBlue + `)
    VALUES ($1, $2, $3, $4, $5)
    RETURNING id`

const countTasksBlueSQL = `
    SELECT count(id)
    FROM task_manager_blue`

const findTaskBlueSQL = `
    SELECT id, ` + columnsTaskManagerBlue + `
    FROM task_manager_blue
    WHERE id = $1`

const findAllTasksBlueSQL = `
    SELECT id, ` + columnsTaskManagerBlue + `
    FROM task_manager_blue`

const findAllTasksByTypeAndStatusBlueSQL = `
    SELECT id, ` + columnsTaskManagerBlue + `
    FROM task_manager_blue
    WHERE task_type = $1 AND status = $2`

const updateTaskBlueSQL = `
    UPDATE task_manager_blue
    SET (` + columnsTaskManagerBlue + `) =
    ($2, $3, $4, $5, $6)
    WHERE id = $1`

const deleteTaskBlueSQL = `
    DELETE FROM task_manager_blue
    WHERE id = $1`

func (m *TaskManager) createTaskBlue(t Task) (Task, error) {
	if t.Timeout < 1 {
		t.Timeout = -1
	}
	t.Status = "Created"

	var id int
	row := m.db.QueryRow(createTaskBlueSQL, rowSourceTask(t)...)
	err := row.Scan(&id)
	if err != nil {
		return Task{}, err
	}

	t.Id = id
	return t, nil
}

func (m *TaskManager) countAllTasksBlue() (int, error) {
	row := m.db.QueryRow(countTasksBlueSQL)

	var count int
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (m *TaskManager) findTaskBlue(id int) (Task, error) {
	row := m.db.QueryRow(findTaskBlueSQL, id)

	var t pqTaskManagerBlue
	err := row.Scan(t.rowDestination()...)
	if err != nil {
		return Task{}, err
	}
	return t.task(), nil
}

func (m *TaskManager) findAllTasksBlue(options map[string]string) ([]Task, error) {
	queryOptions, err := findAllOptionsString(options)
	rows, err := m.db.Query(findAllTasksBlueSQL + queryOptions)

	var result []Task
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t pqTaskManagerBlue
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

func (m *TaskManager) findAllTasksByTypeAndStatusBlue(taskType string, status string, options map[string]string) ([]Task, error) {
	queryOptions, err := findAllOptionsString(options)
	rows, err := m.db.Query(findAllTasksByTypeAndStatusBlueSQL+queryOptions, taskType, status)

	var result []Task
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t pqTaskManagerBlue
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

func (m *TaskManager) updateTaskBlue(t Task) error {
	if t.Timeout < 1 {
		t.Timeout = -1
	}

	_, err := m.db.Exec(updateTaskBlueSQL, append([]interface{}{t.Id}, rowSourceTask(t)...)...)
	return err
}

func (m *TaskManager) deleteTaskBlue(id int) error {
	_, err := m.db.Exec(deleteTaskBlueSQL, id)
	return err
}
