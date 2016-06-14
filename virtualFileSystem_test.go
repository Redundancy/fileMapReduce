package mapreduce

import "testing"

type expected map[string]bool

func expect(items ...string) expected {
	r := expected{}
	for _, item := range items {
		r[item] = false
	}
	return r
}

func (e expected) Check(t *testing.T, entries []string) {
	if len(entries) != len(e) {
		t.Error("Wrong number of items in entries:", entries)
	}

	for _, entry := range entries {
		if _, found := e[entry]; found {
			e[entry] = true
		} else {
			t.Error("Unexpected entry:", entry)
		}
	}

	for entry, found := range e {
		if !found {
			t.Error("Missing entry:", entry)
		}
	}
}

func TestVFSFolderList(t *testing.T) {
	fs := StaticVirtualFileSystem{
		"folder/folder/file.txt":          "content",
		"folder/folder/file2.txt":         "more content",
		"folder/other/something/file.txt": "content2",
	}

	folders, _, _ := fs.List("folder")
	expect("folder", "other").Check(t, folders)
}

func TestVFSFileList(t *testing.T) {
	fs := StaticVirtualFileSystem{
		"folder/folder/file.txt":          "content",
		"folder/folder/file2.txt":         "more content",
		"folder/other/something/file.txt": "content2",
	}

	_, files, _ := fs.List("folder/folder")
	expect("file.txt", "file2.txt").Check(t, files)
}

func TestVFSFileOpen(t *testing.T) {
	fs := StaticVirtualFileSystem{
		"folder/folder/file.txt":          "content",
		"folder/folder/file2.txt":         "more content",
		"folder/other/something/file.txt": "content2",
	}

	content, err := fs.Open("folder/folder/file.txt")
	if err != nil {
		t.Error(err)
	}
	if content != "content" {
		t.Errorf("Content of file unexpected: %v", content)
	}
}
