package taskmanager

import (
	"strconv"
)

func (m *TaskManager) CreateTask(t Task) (int, error) {
	return m.createTaskBlue(t)
}

func (m *TaskManager) CountAllTasks() (int, error) {
	return m.countAllTasksBlue()
}

func (m *TaskManager) FindTask(id int) (Task, error) {
	return m.findTaskBlue(id)
}

func (m *TaskManager) FindAllTasks(options map[string]string) ([]Task, error) {
	return m.findAllTasksBlue(options)
}

func (m *TaskManager) FindAllTasksByTypeAndStatus(taskType string, status string, options map[string]string) ([]Task, error) {
	return m.findAllTasksByTypeAndStatusBlue(taskType, status, options)
}

func (m *TaskManager) UpdateTask(t Task) error {
	return m.updateTaskBlue(t)
}

func (m *TaskManager) DeleteTask(id int) error {
	return m.deleteTaskBlue(id)
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
