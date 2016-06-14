package mapreduce

import (
	"fmt"
	"strings"
)

// StaticVirtualFileSystem is a simple implement a file system for testing
type StaticVirtualFileSystem map[string]interface{}

// List the folders and files under a path
func (s StaticVirtualFileSystem) List(path string) (folders []string, files []string, err error) {
	folderSet := map[string]bool{}
	files = make([]string, 0, len(s))

	for filename := range s {
		if strings.HasPrefix(filename, path) {
			remaining := strings.TrimLeft(filename[len(path):], "/")
			if strings.Contains(remaining, "/") {
				folder := remaining[0:strings.Index(remaining, "/")]
				folderSet[folder] = true
			} else {
				files = append(files, remaining)
			}
		}
	}

	folders = make([]string, 0, len(folderSet))
	for folder := range folderSet {
		folders = append(folders, folder)
	}
	return folders, files, nil
}

// Open a file - the contents can be anything that a Map function will accept
func (s StaticVirtualFileSystem) Open(path string) (contents interface{}, err error) {
	var found bool
	if contents, found = s[path]; !found {
		return nil, fmt.Errorf("File not found: %v", path)
	}
	return contents, nil
}
