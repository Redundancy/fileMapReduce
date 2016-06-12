package mapreduce

type folderToMap struct {
	// The relative path of the folder to the root
	Path string
	// The jobs that are potentially applicable to the folder
	Jobs Jobs
}

type jobAndDirectoryStack struct {
	Job
	stack [][]byte
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

func (w *workRemaining) addSubfoldersToRemainingWork(currentFolder folderToMap, folders []string) {
	for _, folder := range folders {
		subfolder := joinWithSlash(currentFolder.Path, folder)
		potentialJobsForSubfolder := currentFolder.Jobs.Potential(subfolder)

		if len(potentialJobsForSubfolder) > 0 {
			w.Add(folderToMap{
				subfolder,
				potentialJobsForSubfolder,
			})
		}
	}
}
