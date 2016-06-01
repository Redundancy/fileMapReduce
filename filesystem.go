package mapreduce

import (
	"io/ioutil"
	"path/filepath"
)

// FS is a basic file system starting at a given Root folder
type FS struct {
	Root string
}

// List the names of subfolders and files in a path
func (f *FS) List(path string) (folders []string, files []string, err error) {
	p := filepath.Join(f.Root, path)
	info, err := ioutil.ReadDir(p)

	if err != nil {
		return nil, nil, err
	}

	folders = make([]string, 0, len(info))
	files = make([]string, 0, len(info))

	for _, i := range info {
		if i.IsDir() {
			folders = append(folders, i.Name())
		} else {
			files = append(files, i.Name())
		}
	}

	return folders, files, err
}

// Open a file in the file system and read it
func (f *FS) Open(path string) (contents []byte, err error) {
	p := filepath.Join(f.Root, path)
	return ioutil.ReadFile(p)
}
