package mapreduce

import (
	"container/heap"
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"
)

//go:generate go run github.com/vektra/mockery/v2@v2.43.2 --all

type Storage interface {
	OpenInputFile(name string) (InputFile, error)
	CreateOutputFile(name string) (OutputFile, error)
}

type InputFile interface {
	Close() error
	Scan() bool
	ReadLine() string
	ReadMappedLine() (string, int, error)
	Err() error
}

type OutputFile interface {
	Close() error
	Write(line string) error
}

type Service struct {
	n       int
	workers int
	storage Storage
}

func NewService(n, workers int, storage Storage) *Service {
	return &Service{
		n:       n,
		workers: workers,
		storage: storage,
	}
}

func (s *Service) Do(ctx context.Context, inputFileName string) (string, error) {
	tempFiles, err := s.MapAndShuffle(ctx, inputFileName)
	if err != nil {
		return "", fmt.Errorf("map and shuffle stage failed, error=%w", err)
	}

	outputFileName, err := s.reduce(ctx, tempFiles)
	if err != nil {
		return "", fmt.Errorf("reduce stage failed, error=%w", err)
	}

	return outputFileName, nil
}

func (s *Service) MapAndShuffle(ctx context.Context, inputFileName string) (tempFiles []string, err error) {
	inputFile, err := s.storage.OpenInputFile(inputFileName)
	if err != nil {
		return nil, fmt.Errorf("open input file failed, error=%w", err)
	}
	defer func() {
		if err := inputFile.Close(); err != nil {
			errors.Join(err, fmt.Errorf("failed to close input. Err=%w", err))
		}
	}()

	wordCount := make(map[string]int)
	fileIndex := 0

	for inputFile.Scan() {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled, err if any=%w", ctx.Err())
		default: // just continue
		}

		word := inputFile.ReadLine()
		if word == "" {
			continue
		}
		wordCount[word]++

		if len(wordCount) >= s.n {
			tempFile, err := s.shuffleAndSendToWorker(ctx, wordCount, fileIndex)
			if err != nil {
				return nil, fmt.Errorf("shuffleAndSendToWorker failed, error=%w", err)
			}
			tempFiles = append(tempFiles, tempFile)
			clear(wordCount)
			wordCount = make(map[string]int)
			fileIndex++
		}
	}

	if len(wordCount) > 0 {
		tempFile, err := s.shuffleAndSendToWorker(ctx, wordCount, fileIndex)
		if err != nil {
			return nil, fmt.Errorf("shuffleAndSendToWorker failed, error=%w", err)
		}
		tempFiles = append(tempFiles, tempFile)
		clear(wordCount)
	}

	return tempFiles, nil
}

func (s *Service) shuffleAndSendToWorker(ctx context.Context, wordCount map[string]int, fileIndex int) (tempFileName string, err error) {
	tempFileName = fmt.Sprintf("temp_%d.tsv", fileIndex)
	writer, err := s.storage.CreateOutputFile(tempFileName)
	if err != nil {
		return "", fmt.Errorf("create temp file failed, error=%w", err)
	}
	defer func() {
		closeErr := writer.Close()
		if closeErr != nil {
			err = errors.Join(fmt.Errorf("close temp file failed, err=%w", err))
		}
	}()

	// we don't duplicate words here, since string is just a pointer to char/rune array
	words := make([]string, 0, len(wordCount))
	for word := range wordCount {
		words = append(words, word)
	}
	sortInPlace(&words)

	// flush to file
	for _, word := range words {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("context cancelled, error if any=%w", ctx.Err())
		default:
		}
		line := fmt.Sprintf("%s\t%d\n", word, wordCount[word])
		err := writer.Write(line)
		if err != nil {
			return "", fmt.Errorf("temp file write line failed, error=%w", err)
		}
	}

	return tempFileName, nil
}

func (s *Service) openReadFiles(tempFiles []string) ([]InputFile, error) {
	res := make([]InputFile, len(tempFiles))
	for i, f := range tempFiles {
		inF, err := s.storage.OpenInputFile(f)
		if err != nil {
			return nil, fmt.Errorf("failed to open files in storage, err=%w", err)
		}
		res[i] = inF
	}

	return res, nil
}

func (s *Service) mergeSortedFiles(tempFiles []string, outputFile string) (err error) {
	files, err := s.openReadFiles(tempFiles)
	if err != nil {
		return fmt.Errorf("failed to open files in storage, err=%w", err)
	}
	defer func() {
		for _, f := range files {
			closeErr := f.Close()
			if closeErr != nil {
				err = errors.Join(err, fmt.Errorf("failed to close file, err=%w", closeErr))
			}
		}
	}()

	writer, err := s.storage.CreateOutputFile(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file in storage, err=%w", err)
	}
	defer func() {
		closeErr := writer.Close()
		if closeErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close output file, err=%w", closeErr))
		}
	}()

	// Create min-heap of words
	minHeap := newWordHeap()

	for i, f := range files {
		if f.Scan() {
			word, count, err := f.ReadMappedLine()
			if err != nil {
				return fmt.Errorf("failed to read mapped line from file in storage, err=%w", err)
			}
			heap.Push(minHeap, WordEntry{word: word, count: count, fileIndex: i})
		}
	}

	var prevWord string
	var totalCount int

	for minHeap.Len() > 0 {
		entry := heap.Pop(minHeap).(WordEntry)

		if entry.word == prevWord {
			totalCount += entry.count
		} else {
			if prevWord != "" {
				line := fmt.Sprintf("%s\t%d\n", prevWord, totalCount)
				err := writer.Write(line)
				if err != nil {
					return fmt.Errorf("temp file write line failed, error=%w", err)
				}
			}
			prevWord = entry.word
			totalCount = entry.count
		}

		// Read work from the same file
		f := files[entry.fileIndex]
		if f.Scan() {
			word, count, err := f.ReadMappedLine()
			if err != nil {
				return fmt.Errorf("call ReadMappedLine failed, error=%w", err)
			}
			heap.Push(minHeap, WordEntry{word: word, count: count, fileIndex: entry.fileIndex})
		}
	}

	// Write last word
	if prevWord != "" {
		line := fmt.Sprintf("%s\t%d\n", prevWord, totalCount)
		err := writer.Write(line)
		if err != nil {
			return fmt.Errorf("write failed, error=%w", err)
		}
	}

	return nil
}

func (s *Service) reduce(ctx context.Context, tempFiles []string) (string, error) {
	if len(tempFiles) == 0 {
		return "", fmt.Errorf("nothing to reduce")
	}
	outFileCounter := 0
	for len(tempFiles) > 1 {
		var newFiles []string
		eg := &errgroup.Group{}
		mergeChan := make(chan string, len(tempFiles)/2+1)

		for i := 0; i < len(tempFiles); i += 2 {
			select {
			case <-ctx.Done():
				return "", fmt.Errorf("context cancelled, err if any=%w", ctx.Err())
			default: // just continue
			}
			if i+1 < len(tempFiles) {
				outputFile := fmt.Sprintf("merged_%d.tsv", outFileCounter) //i/2)
				outFileCounter++
				//wg.Add(1)
				//out := outputFile
				//f1 := tempFiles[i]
				//f2 := tempFiles[i+1]
				func(f1, f2, out string) {
					eg.Go(func() error {
						//defer wg.Done()
						err := s.mergeSortedFiles([]string{f1, f2}, out)
						if err != nil {
							return fmt.Errorf("merge failed, err=%w", err)
						}
						mergeChan <- out

						return nil
					})
				}(tempFiles[i], tempFiles[i+1], outputFile)
			} else {
				mergeChan <- tempFiles[i] // one left, done need to merge, pass it to the next merge iteration
			}
		}

		err := eg.Wait()
		if err != nil {
			return "", err // it's ok not to close channel, it'll be GC'ed.
		}
		close(mergeChan) // here we have to close channel to notify reciever below to exot range loop.

		for file := range mergeChan {
			newFiles = append(newFiles, file)
		}

		tempFiles = newFiles
	}

	return tempFiles[0], nil
}
