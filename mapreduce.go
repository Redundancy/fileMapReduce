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
	foldersRemaining := &workRemaining{}
	foldersRemaining.Add(folderToMap{Path: "", Jobs: jobs})

	jobWorkResult := make([]chan []MapResult, len(jobs))
	for i := range jobs {
		jobs[i].jobIndex = i
		jobWorkResult[i] = make(chan []MapResult)
	}

	// stop is the gordian knot used to break all deadlocks
	// all sends are done as selects against this channel, allowing them to
	// drain when it is closed
	stop := make(chan struct{})
	stopNow := (&once{f: func() { close(stop) }}).Do

	defer stopNow()
	// All blocking sends and receives on the main channel must listen to the
	// error channel. Only the first error is received from it
	errorChannel := make(chan error)

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
		go fileOpener(
			filesystem,
			fileOpenRequests,
			mapperInput,
			errorChannel,
			stop,
			&ioWaitGroup,
		)

		go mappingApplier(
			mapperInput,
			stop,
			&mapperWaitGroup,
		)
	}

	for _, job := range jobs {
		aggregatorWaitGroup.Add(1)
		go aggregator(
			jobWorkResult[job.jobIndex],
			job.BatchSize,
			reducerInputs[job.jobIndex],
			stop,
			job.Sorter,
			&aggregatorWaitGroup,
		)
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

		for _, d := range jobWorkResult {
			close(d)
		}

		aggregatorWaitGroup.Wait()
		// aggregators done feeding reducers

		for _, d := range reducerInputs {
			close(d)
		}

		reducerWaitGroup.Wait()
		close(done)
	}}).Do

	defer shutdown()
	for !foldersRemaining.Done() {
		currentFolder, _ := foldersRemaining.Pop()
		folders, files, listErr := filesystem.List(currentFolder.Path)

		if listErr != nil {
			return listErr
		}

		foldersRemaining.addSubfoldersToRemainingWork(
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
					m:         job,
					path:      fullpath,
					result:    jobWorkResult[i],
					errorChan: errorChannel,
				}
			}

			fileOpenRequests <- fileOpenRequest{
				path: fullpath,
				work: mappers,
			}
		}
	}

	inputDone = true
	close(fileOpenRequests)
	go shutdown()

	// Note, once we don't block on every reduce at this point,
	// we will have to be very careful about deadlocks between Wait()
	// and sending to the error channels on Reducers etc.

	select {
	case <-done:
		return nil
	case err = <-errorChannel:
		stopNow()
		return err
	}
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
) {
	defer fileWaitGroup.Done()

	for i := range inputChan {
		content, err := fs.Open(i.path)

		if err != nil {
			select {
			case <-stop:
				return
			case errChan <- &fileOpenError{path: i.path, err: err}:
				return
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

type mappingWork struct {
	m         Mapper
	path      string
	content   []byte
	result    chan<- []MapResult
	errorChan chan<- error
}

func mappingApplier(
	workChan chan mappingWork,
	stop <-chan struct{},
	waiter *sync.WaitGroup,
) {
	defer waiter.Done()

	for work := range workChan {
		r, err := work.m.Map(work.path, work.content)

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

func aggregator(
	results <-chan []MapResult,
	batchSize int,
	reducer chan<- []MapResult,
	cancel <-chan struct{},
	sorter Sorter,
	aggregatorWaitGroup *sync.WaitGroup,
) {
	defer aggregatorWaitGroup.Done()

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
