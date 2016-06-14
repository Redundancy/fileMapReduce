package mapreduce

// Filter is mechanism for filtering files and directories as we recursively descend
type Filter interface {
	// CouldMatch indicates that a directory path could match the filter in the future
	CouldMatch(string) bool

	// Match indicates an actual match with a file
	Match(string) bool
}

// Mapper provides an interface to execute map operations on files
// parents is a slice of the contents of directory files from folders above the path
//
// The objects provided in the parents and data parameters must not be modified by the Map function
// since they are potentially in use within multiple mappers simultaneoussly
type Mapper interface {
	Map(path string, parents []interface{}, data interface{}) ([]MapResult, error)
}

// FunctionMapper implements the Mapper interface with a simple function
type FunctionMapper func(path string, parents []interface{}, data interface{}) ([]MapResult, error)

// Map implements Mapper.Map
func (f FunctionMapper) Map(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
	return f(path, parents, data)
}

// Sorter (if provided as part of a Job) will ensure that each invocation of
// Reduce receives items in sorted order
type Sorter interface {
	Less(o *MapResult, than *MapResult) bool
}

// FunctionSorter implements Sorter for a function
type FunctionSorter func(o *MapResult, than *MapResult) bool

// Less implements Sorter.Less
func (f FunctionSorter) Less(o *MapResult, than *MapResult) bool {
	return f(o, than)
}

func simpleLess(o *MapResult, than *MapResult) bool {
	return o.Key < than.Key
}

// SimpleLess implements a default sorting (ascending)
var SimpleLess = FunctionSorter(simpleLess)

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
	// List the folders and files under a path
	List(path string) (folders []string, files []string, err error)
	// Open a file - the contents can be anything that a Map function will accept
	Open(path string) (contents interface{}, err error)
}
