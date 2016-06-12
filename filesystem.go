package mapreduce

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

// LoaderFunc is a function that loads data into a datatype once per file
// This consolidates the cost of dealing with structured data
type LoaderFunc func(path string, r io.Reader) (loaded interface{}, err error)

// FS is a basic file system starting at a given Root folder
type FS struct {
	Root string
	// Optional function to load files
	Loader LoaderFunc
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

func nilLoader(path string, r io.Reader) (loaded interface{}, err error) {
	return ioutil.ReadAll(r)
}

// Open a file in the file system and read it
func (f *FS) Open(path string) (contents interface{}, err error) {
	loader := f.Loader

	if f.Loader == nil {
		loader = nilLoader
	}
	fullpath := filepath.Join(f.Root, path)

	file, err := os.Open(fullpath)

	if err != nil {
		return
	}
	defer file.Close()

	return loader(path, file)
}
