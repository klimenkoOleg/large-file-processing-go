# Approach

I implemented a MapReduce-style architecture to process large files efficiently. The process consists of three main stages:

## Mapping & Shuffling:
        The [MapAndShuffle|https://github.com/klimenkoOleg/large-file-processing-go/blob/main/internal/domain/mapreduce/service.go#L61]  function reads the input file and uses a standard map to count word frequencies.
        Once the map size exceeds N unique words, its contents are flushed to a temporary file.
        Before writing, each batch is sorted in-place in alphabetical order to optimize the merging step.

## Reducing (Parallel Merging):
        The [reduce||https://github.com/klimenkoOleg/large-file-processing-go/blob/main/internal/domain/mapreduce/service.go#L281] function spawns Goroutines to merge temporary files in parallel.
        Each Goroutine merges two files at a time, but the batch size can be increased for better performance.
        The merging algorithm follows a K-way merge strategy using a Min Heap:
            Open a pair of files and read them word by word, inserting words into the Min Heap.
            Once the heap size exceeds N, flush it to a new merged file and repeat the process.

## Final Output:
        The final result is a fully sorted TSV file, where words appear in alphabetical order along with their frequencies.
        This sorting behavior aligns with the project requirements.



# Usage     
To init:
```console
make setup 
```

To run:
```console
make run 
```