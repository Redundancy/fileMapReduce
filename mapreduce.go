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

// MapReduce takes a list of Jobs and executes them concurrently on
// a FileSystem
func MapReduce(filesystem FileSystem, jobs Jobs) (err error) {
	foldersRemaining := &workRemaining{}
	foldersRemaining.Add(folderToMap{jobs, ""})

	jobWorkResult := make([]chan mappingWorkResult, len(jobs))
	for i := range jobs {
		jobs[i].jobIndex = i
		jobWorkResult[i] = make(chan mappingWorkResult)
	}

	// stop is the gordian knot used to break all deadlocks
	// all sends are done as selects against this channel, allowing them to
	// drain when it is closed
	stop := make(chan struct{})
	stopped := false
	stopNow := func() {
		if !stopped {
			close(stop)
			stopped = true
		}
	}
	defer stopNow()
	// All blocking sends and receives on the main channel must listen to the
	// error channel. Only the first error is received from it
	errorChannel := make(chan error)

	reducerWaitGroup := sync.WaitGroup{}
	mapperWaitGroup := sync.WaitGroup{}
	ioWaitGroup := sync.WaitGroup{}

	fileOpenRequests := make(chan fileOpenRequest, 2*runtime.NumCPU())
	mapperInput := make(chan mappingWork)
	reducerInputs := startReducers(
		jobs,
		stop,
		errorChannel,
		&reducerWaitGroup,
	)

	numCPU := runtime.NumCPU()
	ioWaitGroup.Add(numCPU)
	mapperWaitGroup.Add(numCPU)

	for i := 0; i < numCPU; i++ {
		go fileOpener(
			filesystem,
			fileOpenRequests,
			mapperInput,
			errorChannel,
			stop,
			&ioWaitGroup,
			&mapperWaitGroup,
		)

		go mappingApplier(
			mapperInput,
			stop,
			&mapperWaitGroup,
		)
	}

	defer func() {
		// shut down the pipline in order
		close(fileOpenRequests)
		ioWaitGroup.Wait()
		// done reading files and feeding the mapper

		close(mapperInput)
		mapperWaitGroup.Wait()
		// mappers done feeding reducers

		for _, d := range reducerInputs {
			close(d)
		}

		reducerWaitGroup.Wait()
	}()

	for !foldersRemaining.Done() {
		currentFolder, _ := foldersRemaining.Pop()
		folders, files, listErr := filesystem.List(currentFolder.Path)

		if listErr != nil {
			return listErr
		}

		addSubfoldersToRemainingWork(
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

			mappers := make([]mappingWork, len(applicableJobs))
			for i, job := range applicableJobs {
				mappers[i] = mappingWork{
					m:      job,
					path:   fullpath,
					result: jobWorkResult[i],
				}
			}

			fileOpenRequests <- fileOpenRequest{
				path: fullpath,
				work: mappers,
			}

			// TODO: Let the mappers feed the reducers directly
			for i, job := range applicableJobs {
				var res mappingWorkResult

				select {
				case res = <-jobWorkResult[i]:
				case err = <-errorChannel:
					stopNow()
					return err
				}

				results, resultErr := res.results, res.err

				if resultErr != nil {
					err = resultErr
					stopNow()
					return err
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

	return err
}

type fileOpenRequest struct {
	path string
	work []mappingWork
}

func fileOpener(
	fs FileSystem,
	inputChan <-chan fileOpenRequest,
	outputChan chan<- mappingWork,
	errChan chan<- error,
	stop <-chan struct{},
	fileWaitGroup *sync.WaitGroup,
	mapperWaitGroup *sync.WaitGroup,
) {
	defer fileWaitGroup.Done()

	for i := range inputChan {
		content, err := fs.Open(i.path)

		if err != nil {
			select {
			case <-stop:
				return
			case errChan <- err:
				return
			}
		}

		for _, work := range i.work {
			work.content = content

			// don't block in case stopped
			select {
			case <-stop:
				return
			case outputChan <- work:
			}

		}
	}
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
}

func mappingApplier(
	workChan chan mappingWork,
	stop <-chan struct{},
	waiter *sync.WaitGroup,
) {
	defer waiter.Done()

	for work := range workChan {
		r, err := work.m.Map(work.path, work.content)
		select {
		case <-stop:
			return
		case work.result <- mappingWorkResult{r, err}:
		}
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

func addSubfoldersToRemainingWork(work *workRemaining, currentFolder folderToMap, folders []string) {
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

func startReducers(
	jobs Jobs,
	stop <-chan struct{},
	errorChannel chan<- error,
	wg *sync.WaitGroup,
) (dispatchers []chan<- []MapResult) {
	// for each job, create a dispatcher channel
	dispatchers = make([]chan<- []MapResult, len(jobs))

	for i := range jobs {
		resultChannel := make(chan []MapResult, 5)
		dispatchers[i] = resultChannel

		job := jobs[i]
		reducerFunc := job.Reducer

		if reducerFunc != nil {
			wg.Add(1)
			go reduceAndFinalize(
				job,
				resultChannel,
				stop,
				errorChannel,
				wg,
			)
		}
	}

	return dispatchers
}

func reduceAndFinalize(
	job Job,
	resultChannel chan []MapResult,
	stop <-chan struct{},
	errorChannel chan<- error,
	wg *sync.WaitGroup,
) {
	result, err := job.Reducer.Reduce(nil, resultChannel)

	if err == nil && job.Finalizer != nil {
		err = job.Finalizer.Finish(result)
	}

	if err != nil {
		select {
		case <-stop:
		case errorChannel <- err:
		}
	}

	wg.Done()
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
