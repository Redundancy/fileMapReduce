# fileMapReduce

fileMapReduce exists as a way to apply concepts of MapReduce on a file system, while optimizing in particular the use-case of wanting to run multiple MapReduce functions in concurrently without the overhead of traversing the file system in multiple passes.

It is based on work that I [published](https://medium.com/@offbyone/file-static-data-e2af8f8e8c0a#.lknuhhfma) while at CCP Games. While the original library was quite general use, it required Python's multi-processing library to make effective use of all the cores available (and I never had time to prepare it to be open-source).

## Why?
Build Systems can always benefit from being able to use more CPU cores, and using well established concepts to do so. Golang lends itself to the task much better than Python. Ultimately, it would be nice to be able to make a potentially distributed system rather than relying on a single machine with huge IO and lots of cores.

When originally building the static data system called FSD (File Static Data) for Eve Online, we determined that on a heavily used dataset that was organized by editing context, each file would be relevant to many potential outputs, typically dictionaries - what is station X, what's in solar system Y, what does moon Z look like? Parsing and iterating over the entire dataset would be significantly time consuming and push the processing time for the dataset into hours (totally impractical for any decent form of iteration). In order to build this data we would stream output into sorted lists and merge the results to produce a efficient and large lookup tables in files.

Running multiple builds concurrently in isolation, while consolidating the cost of the IO and parsing was the best way to reduce the cost in a "from scratch" build.

The other advantage of the original library was that it could build gigabytes of data and thousands of files in a 32 bit process by streaming intermediate results to disk then efficiently merging the results.

### What's with the hierarchy?
In a MapReduce process, you're usually working on keys and documents in a flat table, that's not always the case with data from source control or disk.

When working on a file system, the following must be taken into account:
* Your file system is a hierarchy you can take advantage of for data organization
 * When you do data under organized in a subfolder may need meta-data beyond just the folder name
 * Data should be expressed uniquely in an editing system to prevent inconsistency
* The file system, tools and humans don't tend to deal well with thousands of files in one directory

If you build a MapReduce process for a file system, it should allow you to take advantage of the file system as naturally as possible. The approach taken is to allow so-called "directory" files, which are passed to mappers alongside the value.

### How do I use this?
To use this system most effectively, you should probably have read my ideas about the design motivations and considerations for static data in source control [in my blog](https://medium.com/@offbyone/file-static-data-e2af8f8e8c0a#.lknuhhfma).

Since this is primarily a library, the more specific answer will be based on the tool, if it ever gets created. At the moment, the library isn't fit for consumption (pin the version and anticipate change and issues). At the moment, the best documentation on usage are the tests.

### Golang?
My goal would be to use a plugin system that allows Golang to spawn up worker processes (in any language) and feed them. This would be part of a tool based on top of the library, if ever done, and I would probably start with Python.

Golang is a natural fit for this problem - it's very easy to deploy, it's one of the fastest languages available while being high level and having all the primitives that you could want for using cores and managing IO asynchrony.

## Status
In development (at my own pace). APIs are likely to change and code is going to get reorganized as I work on features.

### TODO List
- [ ] Improve TODO list :)
- [ ] Streaming finalization - finalize with a stream of incrementally loaded data
- [ ] Improve how reduce is applied (batch sizes?)
- [ ] Clear rules for reduction - are we aiming for a single stateful reducer or are we potentially distributing reduction work to multiple cores?
- [x] Loading of parents based on directory file filters
- [x] Pipeline file loading (more than one load happening at once). Is this needed / faster if the map and reduce functions are on other threads?
- [x] Loader - transform inputs before they are passed to mapping functions (eg.  msgpack for lower cost marshalling / unmarshalling)
- [ ] (Low) Messaging support (eg NATS) for distributing workloads?
