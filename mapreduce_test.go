package mapreduce

import (
	"bytes"
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

	var f1 FunctionMapper = func(path string, data []byte) ([]MapResult, error) {
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

func TestLineCounter(t *testing.T) {
	fs := &FS{Root: "testFixtures"}

	var m FunctionMapper = func(path string, data []byte) ([]MapResult, error) {
		newlineCount := bytes.Count(data, []byte{'\n'})
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

	var m FunctionMapper = func(path string, data []byte) ([]MapResult, error) {
		buf := bytes.NewBuffer(data)
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
