package taskmanager

import (
	"database/sql"
	_ "github.com/lib/pq"
	"io/ioutil"
)

const TaskManagerTestDataUrl = ""

func newDBConnection(url string) (*sql.DB, error) {
	// Open Connection
	return sql.Open("postgres", url)
}

func createTableTaskManager() error {
	createTableSQL, err := ioutil.ReadFile("create_task_manager.sql")
	if err != nil {
		return err
	}

	d, err := newDBConnection(TaskManagerTestDataUrl)
	if err != nil {
		return err
	}

	_, err = d.Exec(string(createTableSQL))
	if err != nil {
		return err
	}
	_ = d.Close()

	return nil
}

func dropTableTaskManager() error {
	dropTableSQL := `
        DROP TRIGGER set_task_manager_updated_at_timestamp ON task_manager;
        DROP FUNCTION get_updated_at_timestamp();
        DROP TABLE task_manager;`

	d, err := newDBConnection(TaskManagerTestDataUrl)
	if err != nil {
		return err
	}

	_, err = d.Exec(dropTableSQL)
	if err != nil {
		return err
	}
	_ = d.Close()

	return nil
}
