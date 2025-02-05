package mapreduce

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"
)

type Storage interface {
	OpenInputFile(name string) (*InputFileImpl, error)
	CreateOutputFile(name string) (*OutputFileImpl, error)
}

type InputFile interface {
	Close()
	Scan() bool
	ReadLine() string
	ReadLine() string
	ReadMappedLine() (string, int, error)
	Err() error
}

type OutputFile interface {
	Close()
	WriteLine(line string) error
}

type StorageImpl struct {
	// OpenInputFile(name string) (*InputFile, error)
	// CreateOutputFile(name string) (*OutputFile, error)
}

type Service struct {
	n       int
	workers int
	storage Storage
	heap    *WordHeap
	log     zap.Logger
}

func (s *StorageImpl) OpenInputFile(name string) (*InputFileImpl, error) {
	return newInputFile(name)
}

func (s *StorageImpl) CreateOutputFile(name string) (*InputFileImpl, error) {
	return newOutputFile(name)
}

type InputFileImpl struct {
	inputScanner *bufio.Scanner
}

func newInputFile(name string) (*InputFileImpl, error) {
	inputFile, err := os.Open(name)
	if err != nil {
		return nil
	}
	inputScanner := bufio.NewScanner(inputFile)
	return InputFileImpl{inputScanner: inputScanner}
}

func (s *InputFileImpl) Close() {
	s.file.Close()
}

func (s *InputFileImpl) Scan() bool {
	if s.inputScanner == nil {
		return false
	}
}

func (s *InputFileImpl) ReadLine() string {
	if s.inputScanner == nil {
		return false
	}
}

func (s *InputFileImpl) ReadLine() string {
	if s.inputScanner == nil {
		return false
	}

	return s.inputScanner.Text()
}

func (s *InputFileImpl) ReadMappedLine() (string, int, error) {
	parts := strings.Split(scanner.Text(), "\t")
	count, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("second part in line should be integer, but was not. Error=%w", err)
	}

	return parts[0], count
}

func (s *InputFileImpl) Err() error {
	return s.inputScanner.Err()
}

type OutputFileImpl struct {
	file   *os.File
	writer *bufio.Writer
}

func newOutputFile(fileName string) *OutputFileImpl {
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	writer := bufio.NewWriter(file)

	return &OutputFileImpl{
		file:   file,
		writer: writer,
	}
}

func (s *OutputFileImpl) Close() {
	s.file.Close()
}

func (s *OutputFileImpl) WriteLine(line string) error {
	_, err := fmt.Println(writer, line)
	if err != nil {
		return fmt.Errorf("write to file filed, error=%w", err)
	}

	return nil
}

func main2() {
	defer file.Close()
}

func (s *Service) MapAndShuffle() ([]string, error) {
	const (
		tmpFilePrefix    = "tmp"
		tmpFileExtension = "txt" // without dot!
	)

	inputFile, err := s.storage.OpenInputFile("input.txt")
	if err != nil {
		return nil, fmt.Errorf("open input file failed, error=%w", err)
	}
	defer func() {
		if err := inputFile.Close(); err != nil {
			s.log.Error("Faiuled to close input", zap.Error(err)) // do not send out error, just log it
		}
	}()

	wordCount := make(map[string]int)
	fileIndex := 0
	tempFiles := []string

	for inputFile.Scan() {
		word := inputFile.ReadLine()
		if word == "" {
			continue
		}
		wordCount[word]++

		if len(wordCount) >= s.n {
			tempFile := writeTempFile(wordCount, fileIndex)
			tempFiles = append(tempFiles, tempFile)
			wordCount = make(map[string]int) // Очищаем map
			fileIndex++
		}
	}

	fmt.Println("Созданы временные файлы:", tempFiles)

	return tempFiles, nil

	/*

		var lines []string
		var wg sync.WaitGroup
		tempFiles := make(chan string, s.workers)
		fileIndex := 0

		// Запуск пула горутин
		for inputFile.Scan() {
			lines = append(lines, inputFile.ReadLine())
			if len(lines) >= s.n {
				wg.Add(1)
				go processChunk(lines, fileIndex, &wg, tempFiles)
				fileIndex++
				lines = nil
			}
		}

		// Последний блок
		if len(lines) > 0 {
			wg.Add(1)
			go processChunk(lines, fileIndex, &wg, tempFiles)
		}

		go func() {
			wg.Wait()
			close(tempFiles)
		}()

		// Собираем список файлов
		var tempFileList []string
		for file := range tempFiles {
			tempFileList = append(tempFileList, file)
		}

		fmt.Println("Созданы файлы:", tempFileList)

		return tempFileList, nil*/
}

func processChunk(lines []string, fileIndex int, wg *sync.WaitGroup, tempFiles chan string) {
	defer wg.Done()

	wordCount := make(map[string]int)
	for _, line := range lines {
		wordCount[line]++
	}

	// Сортируем и записываем во временный файл
	tempFileName := fmt.Sprintf("temp_%d.tsv", fileIndex)
	file, _ := os.Create(tempFileName)
	defer file.Close()
	writer := bufio.NewWriter(file)

	words := make([]string, 0, len(wordCount))
	for word := range wordCount {
		words = append(words, word)
	}
	sort.Strings(words)

	for _, word := range words {
		fmt.Fprintf(writer, "%s\t%d\n", word, wordCount[word])
	}
	writer.Flush()

	// Отправляем название файла в канал
	tempFiles <- tempFileName
}

func (s *Service) mergeSortedFiles(tempFiles []string, outputFile string) error {
	files, scanners, err := s.storage.OpenReadFiles(tempFiles)
	if err != nil {
		return fmt.Errorf("Failed to open files in storage", zap.Error(err))
	}

	writer, err := s.storage.OpenWriteFile(outputFile)
	if err != nil {
		return fmt.Errorf("Failed to create output file in storage", zap.Error(err))
	}

	// Create min-heap of words
	minHeap := newWordHeap()

	for i := range scanners {
		if word, count := scanner.Scan(); word != nil { // TODO think about contract, nil
			minHeap.Push(minHeap, WordEntry{word: word, count: frecountquency, fileIndex: i})
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
				fmt.Fprintf(writer, "%s\t%d\n", prevWord, totalCount)
			}
			prevWord = entry.word
			totalCount = entry.count
		}

		// Читаем следующую строку из того же файла
		if word, count := scanners[entry.fileIndex].Scan(); word != nil {
			//parts := strings.Split(scanners[entry.fileIndex].Text(), "\t")
			//count, _ := strconv.Atoi(parts[1])
			heap.Push(minHeap, WordEntry{word: word, count: count, fileIndex: entry.fileIndex})
		}

		// Записываем последнее слово
		if prevWord != "" {
			fmt.Fprintf(writer, "%s\t%d\n", prevWord, totalCount)
		}

		s.storage.FlushAndClose()
	}

	return nil
}

func (s *Service) iterativeMerge(tempFiles []string) string {
	for len(tempFiles) > 1 {
		var newFiles []string
		var wg sync.WaitGroup
		mergeChan := make(chan string, len(tempFiles)/2)

		for i := 0; i < len(tempFiles); i += 2 {
			if i+1 < len(tempFiles) {
				outputFile := fmt.Sprintf("merged_%d.tsv", i/2)
				wg.Add(1)
				go func(f1, f2, out string) {
					defer wg.Done()
					s.mergeSortedFiles([]string{f1, f2}, out)
					mergeChan <- out
				}(tempFiles[i], tempFiles[i+1], outputFile)
			} else {
				mergeChan <- tempFiles[i] // Не с чем сливать, просто передаём дальше
			}
		}
		wg.Wait()
		close(mergeChan)

		for file := range mergeChan {
			newFiles = append(newFiles, file)
		}

		tempFiles = newFiles
	}

	return tempFiles[0]
}

func (s *Service) readFirstWords() ([]string, error) {
	return nil, nil
}
