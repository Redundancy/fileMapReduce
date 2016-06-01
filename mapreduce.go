package mapreduce

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
)

// Job is a single unit of work to produce a single output during a map reduce process
// MapReduce takes multiple jobs and runs them concurrently on the set of files
// ensuring that files are only opened once per MapReduce, rather than once per job
//
// Each Job has an identifying name, and a Filter that chooses what files it will operate on
// Each matching file will be passed to a Mapper which
type Job struct {
	Name      string
	BatchSize uint
	Filter
	Mapper
	Sorter
	Reducer
	Finalizer

	jobIndex int
}

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

// MapReduce takes a list of Jobs and executes them concurrently on
// a FileSystem
func MapReduce(filesystem FileSystem, jobs Jobs) error {
	foldersRemaining := &workRemaining{}
	foldersRemaining.Add(folderToMap{jobs, ""})

	jobWorkResult := make([]chan mappingWorkResult, len(jobs))
	for i := range jobs {
		jobs[i].jobIndex = i
		jobWorkResult[i] = make(chan mappingWorkResult)
	}

	reducerInputs, errorChannel := startReducers(jobs)
	hasFinished := false

	mapperWaitGroup := sync.WaitGroup{}
	mapperInput := make(chan mappingWork)
	for i := 0; i < runtime.NumCPU(); i++ {
		go mappingApplier(mapperInput)
	}

	doneMapping := func() {
		if !hasFinished {
			close(mapperInput)
			mapperWaitGroup.Wait()
			for _, d := range reducerInputs {
				close(d)
			}
			hasFinished = true
		}
	}
	defer doneMapping()

	for !foldersRemaining.Done() {
		currentFolder, _ := foldersRemaining.Pop()
		folders, files, err := filesystem.List(currentFolder.Path)

		if err != nil {
			return err
		}

		addSubfoldersToRemamingWork(
			foldersRemaining,
			currentFolder,
			folders,
		)

	FileMapping:
		for _, filename := range files {
			fullpath := joinWithSlash(currentFolder.Path, filename)
			applicableJobs := currentFolder.Jobs.Matches(fullpath)

			if len(applicableJobs) == 0 {
				continue FileMapping
			}

			// Open once
			content, err := filesystem.Open(fullpath)
			if err != nil {
				return &fileOpenError{
					path: fullpath,
					jobs: applicableJobs.Names(),
					err:  err,
				}
			}

			for i, job := range applicableJobs {
				mapperWaitGroup.Add(1)
				mapperInput <- mappingWork{
					m:       job,
					path:    fullpath,
					content: content,
					result:  jobWorkResult[i],
					waiter:  &mapperWaitGroup,
				}
			} // TODO: move the reducer input to the goroutine

			for i, job := range applicableJobs {
				res := <-jobWorkResult[i]
				results, err := res.results, res.err

				if err != nil {
					return &mappingError{
						path: fullpath,
						job:  job.Name,
						err:  err,
					}
				}

				if len(results) > 1 && job.Sorter != nil {
					sort.Sort(&resultSorter{results, job.Sorter})
				}

				if job.Reducer != nil {
					reducerInputs[job.jobIndex] <- results
				}

			}
		}
	}

	doneMapping()

	var err error

	// Wait for all reducers to finish
	for i := range jobs {
		if jobs[i].Reducer == nil {
			continue
		}

		if err == nil {
			err = <-errorChannel
		} else {
			// ignore further errors
			<-errorChannel
		}
	}

	return err
}

type mappingWorkResult struct {
	results []MapResult
	err     error
}

type mappingWork struct {
	m       Mapper
	path    string
	content []byte
	result  chan<- mappingWorkResult
	waiter  *sync.WaitGroup
}

func (w mappingWork) Apply() {
	r, err := w.m.Map(w.path, w.content)
	w.result <- mappingWorkResult{r, err}
	w.waiter.Done()
}

func mappingApplier(workChan chan mappingWork) {
	for work := range workChan {
		work.Apply()
	}
}

type folderToMap struct {
	Jobs Jobs
	Path string
}

type workRemaining []folderToMap

func (w *workRemaining) Done() bool {
	return len(*w) == 0
}

func (w *workRemaining) Add(r folderToMap) {
	*w = append(*w, r)
}

func (w *workRemaining) Pop() (folderToMap, bool) {
	if len(*w) == 0 {
		return folderToMap{}, false
	}

	popped := (*w)[len(*w)-1]
	*w = (*w)[0 : len(*w)-1]
	return popped, true
}

func addSubfoldersToRemamingWork(work *workRemaining, currentFolder folderToMap, folders []string) {
	for _, folder := range folders {
		subfolder := joinWithSlash(currentFolder.Path, folder)
		potentialJobsForSubfolder := currentFolder.Jobs.Potential(subfolder)

		if len(potentialJobsForSubfolder) > 0 {
			work.Add(folderToMap{
				potentialJobsForSubfolder,
				subfolder,
			})
		}
	}
}

func startReducers(jobs Jobs) (dispatchers []chan<- []MapResult, errors <-chan error) {
	// for each job, create a dispatcher channel
	dispatchers = make([]chan<- []MapResult, len(jobs))
	errorChannel := make(chan error)

	handlers := make([]chan []MapResult, len(jobs))

	for i := range jobs {
		resultChannel := make(chan []MapResult, 5)
		handlers[i], dispatchers[i] = resultChannel, resultChannel

		job := jobs[i]
		reducerFunc := job.Reducer

		if reducerFunc != nil {
			go reduceAndFinalize(job, resultChannel, errorChannel)
		}
	}

	return dispatchers, errorChannel
}

func reduceAndFinalize(job Job, resultChannel chan []MapResult, errorChannel chan<- error) {
	result, err := job.Reducer.Reduce(nil, resultChannel)

	if err != nil {
		errorChannel <- err
	}

	if job.Finalizer != nil {
		errorChannel <- job.Finalizer.Finish(result)
		return
	}

	errorChannel <- nil
}

// Jobs are a list of Jobs
type Jobs []Job

// Names returns a list of Job names
func (j Jobs) Names() []string {
	n := make([]string, len(j))
	for i, job := range j {
		n[i] = job.Name
	}
	return n
}

// Potential returns the list of jobs that could potentially match a path
func (j Jobs) Potential(path string) Jobs {
	results := make(Jobs, 0, len(j))
	for _, job := range j {
		if job.Filter.CouldMatch(path) {
			results = append(results, job)
		}
	}
	return results
}

// Matches returns the jobs that match a path
func (j Jobs) Matches(path string) Jobs {
	results := make(Jobs, 0, len(j))
	for _, job := range j {
		if job.Filter.Match(path) {
			results = append(results, job)
		}
	}
	return results
}

func joinWithSlash(p ...string) string {
	joined := filepath.Join(p...)
	return filepath.ToSlash(joined)
}

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

// Sorts results based on the sorter
type resultSorter struct {
	results []MapResult
	Sorter
}

func (rs *resultSorter) Len() int {
	return len(rs.results)
}

func (rs *resultSorter) Swap(i, j int) {
	rs.results[i], rs.results[j] = rs.results[j], rs.results[i]
}

func (rs *resultSorter) Less(i, j int) bool {
	return rs.Sorter.Less(&rs.results[i], &rs.results[j])
}
