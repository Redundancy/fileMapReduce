package mapreduce

import (
	"path/filepath"
	"runtime"
	"sort"
	"sync"
)

// MapReduce takes a list of Jobs and executes them concurrently on
// a FileSystem
func MapReduce(filesystem FileSystem, jobs Jobs) (err error) {
	aggregatorInputs := make([]chan []MapResult, len(jobs))
	for i := range jobs {
		jobs[i].jobIndex = i
		aggregatorInputs[i] = make(chan []MapResult)
	}

	foldersRemaining := &workRemaining{}
	foldersRemaining.Add(folderToMap{Path: "", jobsAndStack: newStack(jobs)})

	// stop is the gordian knot used to break all deadlocks
	// all sends are done as selects against this channel, allowing them to
	// drain when it is closed
	stop := make(chan struct{})
	stopNow := (&once{f: func() { close(stop) }}).Do

	//defer stopNow()
	// All blocking sends and receives on the main channel must listen to the
	// error channel. Only the first error is received from it
	errorChannel := make(chan error)
	directoryOpenResultChan := make(chan directoryFileOpenResult)

	ioWaitGroup := sync.WaitGroup{}
	mapperWaitGroup := sync.WaitGroup{}
	aggregatorWaitGroup := sync.WaitGroup{}
	reducerWaitGroup := sync.WaitGroup{}

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
		go func() {
			fileOpener(
				filesystem,
				fileOpenRequests,
				mapperInput,
				errorChannel,
				stop,
			)
			ioWaitGroup.Done()
		}()

		go func() {
			mappingApplier(
				mapperInput,
				stop,
			)
			mapperWaitGroup.Done()
		}()
	}

	for _, jobIt := range jobs {
		aggregatorWaitGroup.Add(1)
		job := jobIt
		go func() {
			if job.Reducer == nil {
				discardingAggregator(
					aggregatorInputs[job.jobIndex],
					stop,
				)
			} else {
				aggregator(
					aggregatorInputs[job.jobIndex],
					job.BatchSize,
					reducerInputs[job.jobIndex],
					stop,
					job.Sorter,
				)
			}
			aggregatorWaitGroup.Done()
		}()
	}

	inputDone := false
	done := make(chan struct{})

	shutdown := (&once{f: func() {
		// shut down the pipline from front to back,
		// to give everything a chance to clear
		if !inputDone {
			close(fileOpenRequests)
		}

		ioWaitGroup.Wait()
		// done reading files and feeding the mapper

		close(mapperInput)
		mapperWaitGroup.Wait()
		// mappers done feeding aggregators

		for _, d := range aggregatorInputs {
			if d != nil {
				close(d)
			}
		}

		aggregatorWaitGroup.Wait()
		// aggregators done feeding reducers

		for _, d := range reducerInputs {
			if d != nil {
				close(d)
			}
		}

		reducerWaitGroup.Wait()
		close(done)
	}}).Do

	directoryTracker := &jobTracker{}
	defer shutdown()

	// If we exit early due to an error, teardown before
	// shutdown, since we cannot wait for things to finish normally
	defer func() {
		if err != nil {
			stopNow()
		}
	}()

	for !foldersRemaining.Done() {
		currentFolder, _ := foldersRemaining.Pop()
		folders, files, listErr := filesystem.List(currentFolder.Path)
		directoryFilesInFolderCount := 0

		err = listErr
		if err != nil {
			return err
		}

		for _, filename := range files {
			fullpath := joinWithSlash(currentFolder.Path, filename)
			applicableJobs := currentFolder.Matches(fullpath)

			for _, stack := range currentFolder.jobsAndStack {
				if stack.Job.DirectoryFiles != nil && stack.Job.DirectoryFiles.Match(fullpath) {
					directoryTracker.addJob(stack.Job.jobIndex)
				}
			}

			mappers := make([]mappingWork, len(applicableJobs))
			for i, job := range applicableJobs {
				mappers[i] = mappingWork{
					m:         job.Job,
					path:      fullpath,
					parents:   job.stack,
					result:    aggregatorInputs[i],
					errorChan: errorChannel,
				}
			}

			if directoryTracker.count() > 0 {
				directoryFilesInFolderCount++
			}
			if len(applicableJobs) > 0 || directoryTracker.count() > 0 {
				fileOpenRequests <- fileOpenRequest{
					path: fullpath,
					// The order is important here, since takeJobIndicesForFile will modify the directoryTracker
					directoryFileRequestChan: directoryTracker.getChannelIfLoadingDirectoryFile(directoryOpenResultChan),
					jobIndexes:               directoryTracker.takeJobIndicesForFile(),
					work:                     mappers,
				}
			}
		}

		directoryFileMap := map[int]interface{}{}

		for i := 0; i < directoryFilesInFolderCount; i++ {
			select {
			case result := <-directoryOpenResultChan:
				for _, jobIndex := range result.jobs {
					directoryFileMap[jobIndex] = result.loaded
				}
			case err = <-errorChannel:
				return err
			}
		}

		foldersRemaining.addSubfoldersToRemainingWork(
			currentFolder,
			folders,
			directoryFileMap,
		)
	}

	inputDone = true
	close(fileOpenRequests)
	// start the shutdown in order
	go shutdown()

	// Note, once we don't block on every reduce at this point,
	// we will have to be very careful about deadlocks between Wait()
	// and sending to the error channels on Reducers etc.
	select {
	case <-done:
		return nil
	case err = <-errorChannel:
		return err
	}
}

// jobTracker tracks the jobs that use a file for a directory file
type jobTracker struct {
	jobs []int
}

func (j *jobTracker) addJob(jobIndex int) {
	j.jobs = append(j.jobs, jobIndex)
}

func (j *jobTracker) count() int {
	return len(j.jobs)
}

// gets the slice of job indices and resets
func (j *jobTracker) takeJobIndicesForFile() []int {
	t := j.jobs
	j.jobs = make([]int, 0, 5)
	return t
}

// If there are no directory files to be loaded, return nil instead of the channel
func (j *jobTracker) getChannelIfLoadingDirectoryFile(channel chan directoryFileOpenResult) chan<- directoryFileOpenResult {
	if len(j.jobs) > 0 {
		return channel
	}
	return nil
}

type fileOpenRequest struct {
	path                     string
	work                     []mappingWork
	jobIndexes               []int
	directoryFileRequestChan chan<- directoryFileOpenResult
}

func fileOpener(
	fs FileSystem,
	inputChan <-chan fileOpenRequest,
	outputChan chan<- mappingWork,
	errChan chan<- error,
	stop <-chan struct{},
) {
	for i := range inputChan {

		var err error
		content, err := fs.Open(i.path)

		if err != nil {
			select {
			case <-stop:
				return
			case errChan <- &fileOpenError{path: i.path, err: err}:
				return
			}
		}

		if i.directoryFileRequestChan != nil {
			select {
			case <-stop:
				return
			case i.directoryFileRequestChan <- directoryFileOpenResult{jobs: i.jobIndexes, loaded: content}:
			}
		}

		for _, work := range i.work {
			work.content = content

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

type directoryFileOpenResult struct {
	jobs   []int
	loaded interface{}
}

type mappingWork struct {
	m         Mapper
	path      string
	parents   []interface{}
	content   interface{}
	result    chan<- []MapResult
	errorChan chan<- error
}

func mappingApplier(
	workChan chan mappingWork,
	stop <-chan struct{},
) {
	for work := range workChan {
		r, err := work.m.Map(work.path, work.parents, work.content)

		resultChan := work.result
		errChan := (chan<- error)(nil)

		if err != nil {
			errChan = work.errorChan
			resultChan = nil
		}

		select {
		case <-stop:
			return
		case resultChan <- r:
		case errChan <- err:
			return
		}
	}
}

func discardingAggregator(results <-chan []MapResult, cancel <-chan struct{}) {
	// If there is no reducer channel, just read and discard
	for range results {
		select {
		case <-cancel:
			return
		default:
		}
	}
	return
}

func aggregator(
	results <-chan []MapResult,
	batchSize int,
	reducer chan<- []MapResult,
	cancel <-chan struct{},
	sorter Sorter,
) {

	resultsBatch := make([]MapResult, 0, batchSize)
	for resultGroup := range results {
		if batchSize == 0 {
			select {
			case <-cancel:
				return
			case reducer <- resultGroup:
			}
		} else if len(resultsBatch)+len(resultGroup) >= batchSize {
			take := batchSize - len(resultsBatch)
			resultsBatch = append(resultsBatch, resultGroup[0:take]...)

			if sorter != nil {
				sort.Sort(&resultSorter{resultsBatch, sorter})
			}

			select {
			case <-cancel:
				return
			case reducer <- resultsBatch:
				resultsBatch = make([]MapResult, 0, batchSize)
				resultsBatch = append(resultsBatch, resultGroup[take:len(resultGroup)]...)
			}
		} else {
			resultsBatch = append(resultsBatch, resultGroup...)
		}
	}

	// send remaining
	select {
	case <-cancel:
		return
	case reducer <- resultsBatch:
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

		job := jobs[i]
		reducerFunc := job.Reducer

		if reducerFunc != nil {
			resultChannel := make(chan []MapResult, 5)
			dispatchers[i] = resultChannel

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

func joinWithSlash(p ...string) string {
	joined := filepath.Join(p...)
	return filepath.ToSlash(joined)
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

type once struct {
	done bool
	f    func()
}

func (o *once) Do() {
	if !o.done {
		o.done = true
		o.f()
	}
}
