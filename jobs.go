package mapreduce

// Job is a single unit of work to produce a single output during a map reduce process
// MapReduce takes multiple jobs and runs them concurrently on the set of files
// ensuring that files are only opened once per MapReduce, rather than once per job
//
// Each Job has an identifying name, and a Filter that chooses what files it will operate on
// Each matching file will be passed to a Mapper which
type Job struct {
	Name      string
	BatchSize int
	Filter
	DirectoryFiles Filter
	Mapper
	Sorter
	Reducer
	Finalizer

	jobIndex int
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
