package mapreduce

import (
	"testing"
)

func TestPotentialMatches(t *testing.T) {
	data := map[string]struct {
		filter   PathFilter
		path     string
		expected bool
	}{
		"partial directory":          {Wildcard + "/" + Wildcard, "foo", true},
		"partial match":              {"*o", "foo", true},
		"exact directory":            {"foo/" + Wildcard, "foo", true},
		"non-matching directory":     {"bar/" + Wildcard, "foo", false},
		"matching file":              {Wildcard, "foo.json", true},
		"recursive directory":        {RecursiveWildcard + "/foo.json", "a", true},
		"two directory depth":        {"a/b/foo.json", "a/b", true},
		"non-matching sub directory": {Wildcard + "/c", "a/b", false},
	}

	for k, d := range data {
		if r := d.filter.CouldMatch(d.path); r != d.expected {
			t.Errorf(
				"%v failed: Filter %v on %v - result %v",
				k,
				d.filter,
				d.path,
				r,
			)
		}
	}
}

func TestMatches(t *testing.T) {
	data := map[string]struct {
		filter   PathFilter
		path     string
		expected bool
	}{
		"two wild":               {Wildcard + "/" + Wildcard, "foo/bar.json", true},
		"file match wild":        {Wildcard + ".json", "bar.json", true},
		"no file match":          {Wildcard + ".txt", "bar.json", false},
		"recursive match":        {RecursiveWildcard + "/a.txt", "a/b/c/a.txt", true},
		"too deep":               {"*/*/a.txt", "a/b/c/a.txt", false},
		"recursive and wildcard": {RecursiveWildcard + "/" + Wildcard, "a/b/c/a.txt", true},
		"front recursive":        {"a/" + RecursiveWildcard, "a/b/c/a.txt", true},
		"middle recursive":       {"a/" + RecursiveWildcard + "/c/a.txt", "a/b/c/a.txt", true},
	}

	for k, d := range data {
		if r := d.filter.Match(d.path); r != d.expected {
			t.Errorf(
				"%v failed: Filter %v on %v - result %v",
				k,
				d.filter,
				d.path,
				r,
			)
		}
	}
}

func TestExpectedFailures(t *testing.T) {
	t.Skip("These tests are known failure cases")

	data := map[string]struct {
		filter   PathFilter
		path     string
		expected bool
	}{
		"two wild":        {RecursiveWildcard + "/" + RecursiveWildcard, "foo/bar.json", true},
		"split recursive": {RecursiveWildcard + "/foo/" + RecursiveWildcard, "a/foo/bar.json", true},
	}

	for k, d := range data {
		if r := d.filter.Match(d.path); r != d.expected {
			t.Errorf(
				"%v failed: Filter %v on %v - result %v",
				k,
				d.filter,
				d.path,
				r,
			)
		}
	}
}
