package mapreduce

import (
	"log"
	"regexp"
	"strings"
)

// MultiFilter provides a mechanism for combining filters such that any may match
type MultiFilter []Filter

// CouldMatch implements Filter.CouldMatch
func (m MultiFilter) CouldMatch(p string) bool {
	for _, f := range m {
		if f.CouldMatch(p) {
			return true
		}
	}
	return false
}

// Match implements Filter.Match
func (m MultiFilter) Match(p string) bool {
	for _, f := range m {
		if f.Match(p) {
			return true
		}
	}
	return false
}

// PathFilter implements a basic glob-like filter based on paths
// It uses ** as a recursive directory filter and * as a wildcard
// * may be used partially (eg. *.json) but ** may not
// It could be made more efficient by pre-compiling any regexes needed
type PathFilter string

type matchPrecision bool

const (
	// RecursiveWildcard is the wildcard pattern for PathFilter that matches
	// multiple directories recursively
	RecursiveWildcard = "**"
	// Wildcard is the standard Glob-like wildard, matching any number of characters
	// but not passing over a directory separator
	Wildcard = "*"
	precise  = matchPrecision(false)
	partial  = matchPrecision(true)
)

// CouldMatch takes a partial path and tests if it could potentially match the
// filter with further recursion
func (filter PathFilter) CouldMatch(path string) bool {
	return filter.matchInternal(path, partial)
}

// Match takes a full path relative to the root and tests for an exact match against a filter
func (filter PathFilter) Match(path string) bool {
	return filter.matchInternal(path, precise)
}

func withoutLast(s []string) []string {
	return s[0 : len(s)-1]
}

func withoutFirst(s []string) []string {
	return s[1:len(s)]
}

func (filter PathFilter) matchInternal(path string, precision matchPrecision) bool {
	splitPaths := strings.Split(path, "/")
	splitFilters := strings.Split(string(filter), "/")

	// in general, we compare each filter against a matching directory
	// however, if we hit "**", we avoid it and match anything else until it's
	// the last thing left

	// iteratively shorten the paths and filters until we hit a degenerate case
	for {
		filterLen := len(splitFilters)
		pathLen := len(splitPaths)
		lastPath := pathLen - 1
		lastFilter := filterLen - 1
		pathRemaining := pathLen > 0
		filterRemaining := filterLen > 0

		switch {
		case !filterRemaining && !pathRemaining:
			return true
		case !filterRemaining && pathRemaining:
			return false
		case filterRemaining && !pathRemaining:
			return precision == partial
		case isSingleRecursive(splitFilters):
			return true
		case precision == partial && firstIsRecursive(splitFilters):
			return true
		case !firstIsRecursive(splitFilters):
			// match the front element
			if !matchPartial(splitFilters[0], splitPaths[0]) {
				return false
			}
			splitPaths = withoutFirst(splitPaths)
			splitFilters = withoutFirst(splitFilters)
		case precision == precise && lastIsNotRecursive(splitFilters):
			// match the back element
			if !matchPartial(splitFilters[lastFilter], splitPaths[lastPath]) {
				return false
			}
			splitPaths = withoutLast(splitPaths)
			splitFilters = withoutLast(splitFilters)
		default:
			log.Println("Both **/**!?", filter, path)
			return false
		}
	}
}

func isSingleRecursive(filter []string) bool {
	return len(filter) == 1 && filter[0] == RecursiveWildcard
}

func firstIsRecursive(filter []string) bool {
	return filter[0] == RecursiveWildcard
}

func lastIsNotRecursive(filter []string) bool {
	return filter[len(filter)-1] != RecursiveWildcard
}

func matchPartial(filter, part string) bool {
	switch {
	case filter == Wildcard:
		return true
	case filter == RecursiveWildcard:
		return true
	case filter == part:
		return true
	default:
		return internalMatch(filter, part)
	}
}

// use a regular expression to match
func internalMatch(filter, part string) bool {
	// replace any "." as an escaped "."
	filterAsRegex := strings.Replace(filter, ".", "\\.", -1)
	// replace any "**" as "*"
	// replace any "*" as ".*"
	filterAsRegex = strings.Replace(filterAsRegex, RecursiveWildcard, Wildcard, -1)
	filterAsRegex = strings.Replace(filterAsRegex, Wildcard, ".*", -1)
	r, _ := regexp.Match(filterAsRegex, []byte(part))
	return r
}
