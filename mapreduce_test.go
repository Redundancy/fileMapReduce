package mapreduce

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestFileSystemDirectories(t *testing.T) {
	fs := &FS{Root: "."}
	a, _, err := fs.List("testFixtures/a")

	if err != nil {
		t.Fatal(err)
	}

	if len(a) != 2 {
		t.Error("Expected [b,c]", a)
	}

	if a[0] != "b" {
		t.Error("Expected b:", a[0])
	}
	if a[1] != "c" {
		t.Error("Expected c:", a[1])
	}
}

func TestFileSystemsFiles(t *testing.T) {
	fs := &FS{Root: "."}
	_, a, err := fs.List("testFixtures/a/b")

	if err != nil {
		t.Fatal(err)
	}

	if len(a) != 2 {
		t.Error("Expected [d.txt,e.txt]", a)
	}

	if a[0] != "d.txt" {
		t.Error("Expected d.txt:", a[0])
	}
	if a[1] != "e.txt" {
		t.Error("Expected e.txt:", a[1])
	}
}

func TestMapper(t *testing.T) {
	fs := &FS{Root: "testFixtures"}
	mapped := make([]string, 0, 4)

	var f1 FunctionMapper = func(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
		mapped = append(mapped, path)
		return nil, nil
	}

	MapReduce(fs, []Job{
		Job{
			Name:   "anyC",
			Filter: PathFilter("*/c/*.txt"),
			Mapper: f1,
		},
	})

	if len(mapped) != 1 {
		t.Fatalf("Unexpected number of mapped files: %v", len(mapped))
	}
	if mapped[0] != "a/c/f.txt" {
		t.Errorf("Wrong file path mapped: %v", mapped[0])
	}
}

func sumInt(current interface{}, stream chan []MapResult) (result interface{}, err error) {
	count := int(0)
	if current != nil {
		count += current.(int)
	}
	for results := range stream {
		for _, result := range results {
			count += result.Result.(int)
		}
	}
	return count, nil
}

func singleResult(r interface{}) []MapResult {
	return []MapResult{MapResult{Result: r}}
}

var nilMapper = func(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
	return singleResult(nil), nil
}

var nilFinalizer = func(result interface{}) error {
	return nil
}

func TestLineCounter(t *testing.T) {
	fs := &FS{Root: "testFixtures"}

	var m FunctionMapper = func(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
		newlineCount := bytes.Count(data.([]byte), []byte{'\n'})
		return singleResult(newlineCount), nil
	}

	finalValue := 0
	var f FunctionFinalizer = func(result interface{}) error {
		finalValue = result.(int)
		return nil
	}

	err := MapReduce(fs, []Job{
		Job{
			Name:      "linecounter",
			Filter:    PathFilter("**/*.txt"),
			Mapper:    m,
			Reducer:   FunctionReducer(sumInt),
			Finalizer: f,
		},
	})

	if err != nil {
		t.Error(err)
	}

	if finalValue != 9 {
		t.Error("Unexpected final value", finalValue)
	}
}

func TestLineSum(t *testing.T) {
	fs := &FS{Root: "testFixtures"}

	var m FunctionMapper = func(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
		buf := bytes.NewBuffer(data.([]byte))
		results := make([]MapResult, 0, 5)

		for {
			s, err := buf.ReadString('\n')
			if err != nil {
				break
			}
			i, err := strconv.Atoi(strings.TrimSpace(s))
			if err != nil {
				return nil, err
			}
			results = append(results, MapResult{Result: i})
		}
		return results, nil
	}

	finalValue := 0
	var f FunctionFinalizer = func(result interface{}) error {
		finalValue = result.(int)
		return nil
	}

	err := MapReduce(fs, []Job{
		Job{
			Name:      "sum",
			Filter:    PathFilter("**/*.txt"),
			Mapper:    m,
			Reducer:   FunctionReducer(sumInt),
			Finalizer: f,
		},
	})

	if err != nil {
		t.Error(err)
	}

	if finalValue != 195 {
		t.Error("Unexpected final value", finalValue)
	}
}

type FSError struct {
	FS
}

var errfileSystemOpenFailed = errors.New("TEST FAIL")

func (f *FSError) Open(path string) (contents interface{}, err error) {
	return nil, errfileSystemOpenFailed
}

func TestFileSystemError(t *testing.T) {
	fs := &FSError{FS{Root: "testFixtures"}}

	var m FunctionMapper = func(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
		return singleResult(nil), nil
	}

	var f FunctionFinalizer = func(result interface{}) error {
		return nil
	}

	err := MapReduce(fs, []Job{
		Job{
			Name:      "TestFileSystemError",
			Filter:    PathFilter("**/*.txt"),
			Mapper:    m,
			Reducer:   FunctionReducer(sumInt),
			Finalizer: f,
		},
	})

	if err == nil {
		t.Errorf("Expected an error!")
	}
}

var errMapper = errors.New("TEST FAIL: errMapper")

func TestMapperError(t *testing.T) {
	fs := &FS{Root: "testFixtures"}

	var m FunctionMapper = func(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
		return singleResult(nil), errMapper
	}

	var f FunctionFinalizer = func(result interface{}) error {
		return nil
	}

	err := MapReduce(fs, []Job{
		Job{
			Name:      "TestMapperError",
			Filter:    PathFilter("**/*.txt"),
			Mapper:    m,
			Reducer:   FunctionReducer(sumInt),
			Finalizer: f,
		},
	})

	if err != errMapper {
		t.Error("Expected the file system error!")
	}
}

var errReducer = errors.New("TEST FAIL: errReducer")

func TestReducerError(t *testing.T) {
	fs := &FS{Root: "testFixtures"}

	var m FunctionMapper = nilMapper
	var f FunctionFinalizer = nilFinalizer

	var r FunctionReducer = func(current interface{}, stream chan []MapResult) (result interface{}, err error) {
		return nil, errReducer
	}

	err := MapReduce(fs, []Job{
		Job{
			Name:      "TestReducerError",
			Filter:    PathFilter("**/*.txt"),
			Mapper:    m,
			Reducer:   r,
			Finalizer: f,
		},
	})

	if err != errReducer {
		t.Errorf("Expected the file system error! Got: %#v", err)
	}
}

func TestSortedReduction(t *testing.T) {
	fs := &FS{Root: "testFixtures"}

	var m FunctionMapper = func(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
		return []MapResult{
			MapResult{Key: 3},
			MapResult{Key: 1},
			MapResult{Key: 2},
		}, nil
	}

	var r FunctionReducer = func(current interface{}, stream chan []MapResult) (result interface{}, err error) {
		for items := range stream {
			if !sort.IsSorted(&resultSorter{items, SimpleLess}) {
				err = fmt.Errorf("Unsorted items: %#v", items)
			}
		}
		return nil, err
	}

	var f FunctionFinalizer = nilFinalizer

	err := MapReduce(fs, []Job{
		Job{
			Name:      "TestSortedReduction",
			BatchSize: 3,
			Filter:    PathFilter("**/*.txt"),
			Mapper:    m,
			Sorter:    SimpleLess,
			Reducer:   r,
			Finalizer: f,
		},
	})

	if err != nil {
		t.Error(err)
	}
}

func TestBatching(t *testing.T) {
	fs := &FS{Root: "testFixtures/a"}

	// invoked three times (9 results)
	var m FunctionMapper = func(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
		return []MapResult{
			MapResult{Key: 3},
			MapResult{Key: 1},
			MapResult{Key: 2},
		}, nil
	}

	var r FunctionReducer = func(current interface{}, stream chan []MapResult) (result interface{}, err error) {
		i := 0
		for items := range stream {
			i++
			if i == 1 && len(items) != 5 {
				err = fmt.Errorf("Expected 5 items in first batch %#v", items)
			} else if i == 2 && len(items) != 4 {
				err = fmt.Errorf("Expected 4 items in second batch: %#v", items)
			}
		}
		return nil, err
	}

	var f FunctionFinalizer = nilFinalizer

	err := MapReduce(fs, []Job{
		Job{
			Name:      "TestSortedReduction",
			BatchSize: 5,
			Filter:    PathFilter("**/*.txt"),
			Mapper:    m,
			Sorter:    SimpleLess,
			Reducer:   r,
			Finalizer: f,
		},
	})

	if err != nil {
		t.Error(err)
	}
}

type datatype struct {
	Value int `json:"value"`
}

var jsonFileSystem = &FS{
	Root: "testFixtures/json",
	Loader: func(path string, r io.Reader) (loaded interface{}, err error) {
		data, err := ioutil.ReadAll(r)

		if err != nil {
			return nil, err
		}

		loaded = &datatype{}
		err = json.Unmarshal(data, &loaded)
		return loaded, err
	},
}

func TestLoader(t *testing.T) {
	var m FunctionMapper = func(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
		c, ok := data.(*datatype)
		if !ok {
			t.Errorf("loaded data was not as expected: %#v", data)
		}
		if c.Value != 3 {
			t.Errorf("Unexpected value: %v", c)
		}
		return nil, nil
	}

	err := MapReduce(jsonFileSystem, []Job{
		Job{
			Name:   "TestDir",
			Filter: PathFilter("*/*.json"),
			Mapper: m,
		},
	})

	if err != nil {
		t.Error(err)
	}
}

func TestDirectoryFiles(t *testing.T) {
	var m FunctionMapper = func(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
		item := data.(*datatype)
		if len(parents) != 1 {
			return nil, fmt.Errorf("No parents in mapper!")
		}
		parent := parents[0].(*datatype)
		result := item.Value * parent.Value
		if result != 6 {
			t.Errorf("Wrong result! %v * %v", item, parents[0])
		}
		return singleResult(item.Value * parent.Value), nil
	}

	err := MapReduce(jsonFileSystem, []Job{
		Job{
			Name:           "TestDir",
			Filter:         PathFilter("*/*.json"),
			DirectoryFiles: PathFilter("*.json"),
			Mapper:         m,
		},
	})

	if err != nil {
		t.Error(err)
	}
}

func TestMultipleDirectoryFiles(t *testing.T) {
	fs := StaticVirtualFileSystem{
		"folder/folder/folder/map.txt": "map content",
		"folder/folder/file.txt":       "content",
		"folder/file.txt":              "more content",
		"file.txt":                     "even more content",
	}

	var m FunctionMapper = func(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
		if len(parents) != 3 {
			t.Errorf("Unexpected number of parent files: %v", parents)
		}
		return nil, nil
	}

	err := MapReduce(fs, []Job{
		Job{
			Name:   "TestMultipleDirectoryFiles",
			Filter: PathFilter("**/map.txt"),
			DirectoryFiles: MultiFilter{
				PathFilter("file.txt"),
				PathFilter("**/file.txt"),
			},
			Mapper: m,
		},
	})

	if err != nil {
		t.Error(err)
	}
}

func TestDirectoryFilesWithMultipleJobs(t *testing.T) {
	fs := StaticVirtualFileSystem{
		"folder/folder/folder/map.txt": "map content",
		"folder/folder/file.txt":       "content",
		"folder/file.txt":              "more content",
		"file.txt":                     "even more content",
	}

	var m1 FunctionMapper = func(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
		if len(parents) != 2 {
			t.Errorf("Unexpected number of parent files: %#v", parents)
		}
		return nil, nil
	}

	var m2 FunctionMapper = func(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
		if len(parents) != 1 {
			t.Errorf("Unexpected number of parent files: %#v", parents)
		}
		return nil, nil
	}

	err := MapReduce(fs, []Job{
		Job{
			Name:           "TesDirectoryFilesWithMultipleJobs-2",
			Filter:         PathFilter("**/map.txt"),
			DirectoryFiles: PathFilter("file.txt"),
			Mapper:         m2,
		},
		Job{
			Name:           "TesDirectoryFilesWithMultipleJobs-1",
			Filter:         PathFilter("**/map.txt"),
			DirectoryFiles: PathFilter("**/file.txt"),
			Mapper:         m1,
		},
	})

	if err != nil {
		t.Error(err)
	}
}

type jsonLoader StaticVirtualFileSystem

// List the folders and files under a path
func (s jsonLoader) List(path string) (folders []string, files []string, err error) {
	return StaticVirtualFileSystem(s).List(path)
}

func (s jsonLoader) Open(path string) (contents interface{}, err error) {
	c, err := StaticVirtualFileSystem(s).Open(path)

	if err != nil {
		return nil, err
	}

	loaded := &datatype{}
	data := ([]byte)(c.(string))
	err = json.Unmarshal(data, &loaded)
	return loaded, err
}

func ExampleMapReduce() {
	fs := StaticVirtualFileSystem{
		"file.txt":           "irrelevant file",
		"parent.json":        "{\"value\":2}",
		"folder/child.json":  "{\"value\":3}",
		"folder/child2.json": "{\"value\":4}",
	}

	var multiplyByParent FunctionMapper = func(path string, parents []interface{}, data interface{}) ([]MapResult, error) {
		item := data.(*datatype)
		parent := parents[0].(*datatype)
		return singleResult(item.Value * parent.Value), nil
	}

	var sum FunctionReducer = func(current interface{}, stream chan []MapResult) (result interface{}, err error) {
		i := 0
		if current != nil {
			i = current.(int)
		}

		for items := range stream {
			for _, result := range items {
				i += result.Result.(int)
			}
		}
		return i, nil
	}

	var printResult FunctionFinalizer = func(result interface{}) error {
		fmt.Printf("Sum of Children*Parent: %v", result)
		return nil
	}

	err := MapReduce(jsonLoader(fs), []Job{
		Job{
			Name:           "Example-Sum(Child*Parent)",
			Filter:         PathFilter("*/*.json"),
			DirectoryFiles: PathFilter("*.json"),
			Mapper:         multiplyByParent,
			Reducer:        sum,
			Finalizer:      printResult,
		},
	})

	if err != nil {
		fmt.Println("Error:", err)
	}

	// Output:
	// Sum of Children*Parent: 14
}
