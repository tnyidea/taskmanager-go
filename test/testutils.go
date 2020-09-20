package test

import (
	"database/sql"
	"io/ioutil"
)

const TaskManagerTestDataUrl = ""

func newDBConnection(url string) (*sql.DB, error) {
	// Open Connection
	return sql.Open("postgres", url)
}

func createTableTaskManagerBlue() error {
	createTableTaskManagerBlueSQL, err := ioutil.ReadFile("create_task_manager_blue.sql")
	if err != nil {
		return err
	}

	d, err := newDBConnection(TaskManagerTestDataUrl)
	if err != nil {
		return err
	}

	_, err = d.Exec(string(createTableTaskManagerBlueSQL))
	if err != nil {
		return err
	}
	_ = d.Close()

	return nil
}

func dropTableTaskManagerBlue() error {
	dropTableTaskManagerBlueSQL := `
        DROP TRIGGER set_task_manager_blue_updated_at_timestamp ON task_manager_blue;
        DROP FUNCTION get_updated_at_timestamp();
        DROP TABLE task_manager_blue;`

	d, err := newDBConnection(TaskManagerTestDataUrl)
	if err != nil {
		return err
	}

	_, err = d.Exec(dropTableTaskManagerBlueSQL)
	if err != nil {
		return err
	}
	_ = d.Close()

	return nil
}
