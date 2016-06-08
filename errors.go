package mapreduce

import "fmt"

type mappingError struct {
	path string
	job  string
	err  error
}

func (err *mappingError) Error() string {
	return fmt.Sprintf(
		"Error mapping content of file \"%v\" for \"%v\": %v",
		err.path,
		err.job,
		err.err,
	)
}

type fileOpenError struct {
	path string
	jobs []string
	err  error
}

func (err *fileOpenError) Error() string {
	return fmt.Sprintf(
		"Error opening file \"%v\" for \"%v\": %v",
		err.path,
		err.jobs,
		err.err,
	)
}
