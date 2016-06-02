package mapreduce

// Filter is mechanism for filtering files and directories as we recursively descend
type Filter interface {
	// CouldMatch indicates that a directory path could match the filter in the future
	CouldMatch(string) bool

	// Match indicates an actual match with a file
	Match(string) bool
}

// Mapper provides an interface to execute map operations on files
type Mapper interface {
	Map(path string, data []byte) ([]MapResult, error)
}

// Sorter (if provided as part of a Job) will ensure that each invocation of
// Reduce receives items in sorted order
type Sorter interface {
	Less(o *MapResult, than *MapResult) bool
}

// FunctionMapper implements the Mapper interface with a simple function
type FunctionMapper func(path string, data []byte) ([]MapResult, error)

// Map implements Mapper.Map
func (f FunctionMapper) Map(path string, data []byte) ([]MapResult, error) {
	return f(path, data)
}

// Reducer can be understood in terms of traditional MapReduce concepts
// it is a function that can be invoked many times, sometimes with existing reduced output as an initial state
type Reducer interface {
	// Reduce is called with nil current when input is straight from the mapper
	// You can assume that stream is sorted in terms of the key if a Sorter is provided
	Reduce(current interface{}, stream chan []MapResult) (result interface{}, err error)
}

// FunctionReducer implements the Reducer interface with a simple function
type FunctionReducer func(current interface{}, stream chan []MapResult) (result interface{}, err error)

// Reduce implements Reducer.Reduce
func (f FunctionReducer) Reduce(current interface{}, stream chan []MapResult) (result interface{}, err error) {
	return f(current, stream)
}

// Finalizer takes the result of the final reduction and does whatever a job needs to do with it
//
type Finalizer interface {
	Finish(result interface{}) error
}

// FunctionFinalizer provides a way to implement a Finalizer with a function
type FunctionFinalizer func(result interface{}) error

// Finish from Finalizer
func (f FunctionFinalizer) Finish(result interface{}) error {
	return f(result)
}

// MapResult represents a single result
//
type MapResult struct {
	// An optional Sort key
	Key    int64
	Result interface{}
}

// FileSystem implements a simple disk-based file system interface
type FileSystem interface {
	List(path string) (folders []string, files []string, err error)
	Open(path string) (contents []byte, err error)
}