package mapreduce

type jobAndDirectoryStack struct {
	Job   Job
	stack []interface{}
}

func newStack(jobs Jobs) []jobAndDirectoryStack {
	r := make([]jobAndDirectoryStack, 0, len(jobs))
	for _, job := range jobs {
		r = append(r, jobAndDirectoryStack{
			Job:   job,
			stack: nil,
		})
	}
	return r
}

func (f folderToMap) Matches(path string) []jobAndDirectoryStack {
	results := make([]jobAndDirectoryStack, 0, len(f.jobsAndStack))
	for _, s := range f.jobsAndStack {
		if s.Job.Filter.Match(path) {
			results = append(results, s)
		}
	}
	return results
}

func (f folderToMap) Potential(path string) []jobAndDirectoryStack {
	results := make([]jobAndDirectoryStack, 0, len(f.jobsAndStack))
	for _, s := range f.jobsAndStack {
		if s.Job.Filter.CouldMatch(path) {
			results = append(results, s)
		}
	}
	return results
}

type folderToMap struct {
	// The relative path of the folder to the root
	Path string

	// The jobs that are potentially applicable to the folder
	// and the directory files associated with the job
	jobsAndStack []jobAndDirectoryStack
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

func (w *workRemaining) addSubfoldersToRemainingWork(
	currentFolder folderToMap,
	folders []string,
	newStackItemsForJobs map[int]interface{},
) {
	for _, folder := range folders {
		subfolder := joinWithSlash(currentFolder.Path, folder)
		potentialJobsForSubfolder := currentFolder.Potential(subfolder)

		if len(potentialJobsForSubfolder) > 0 {
			// Take the previous stacks for these jobs and make new ones
			newstacks := make([]jobAndDirectoryStack, len(potentialJobsForSubfolder))
			for i := range newstacks {
				newstacks[i].Job = potentialJobsForSubfolder[i].Job
				newstacks[i].stack = make([]interface{}, 0, len(currentFolder.jobsAndStack)+1)
				newstacks[i].stack = append(newstacks[i].stack, potentialJobsForSubfolder[i].stack...)

				jobIndex := newstacks[i].Job.jobIndex
				if newItem, found := newStackItemsForJobs[jobIndex]; found {
					newstacks[i].stack = append(
						newstacks[i].stack,
						newItem,
					)
				}
			}

			w.Add(folderToMap{
				Path:         subfolder,
				jobsAndStack: newstacks,
			})
		}
	}
}
