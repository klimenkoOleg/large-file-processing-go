package mapreduce

import (
	"bufio"
	"container/heap"
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"
)

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
	log     *zap.Logger
}

func NewService(n, workers int, log *zap.Logger) *Service {
	return &Service{
		n:       n,
		workers: workers,
		storage: NewStorageImpl(),
		log:     log.Named("MapReduceService"),
	}
}

func NewStorageImpl() *StorageImpl {
	return &StorageImpl{}
}

func (s *StorageImpl) OpenInputFile(name string) (InputFile, error) {
	return newInputFile(name)
}

func (s *StorageImpl) CreateOutputFile(name string) (OutputFile, error) {
	return newOutputFile(name)
}

type InputFileImpl struct {
	inputFile    *os.File
	inputScanner *bufio.Scanner
}

func newInputFile(name string) (InputFile, error) {
	inputFile, err := os.Open(name)
	if err != nil {
		return nil, fmt.Errorf("newInputFile filed, error=%w", err)
	}
	inputScanner := bufio.NewScanner(inputFile)

	return &InputFileImpl{
		inputFile:    inputFile,
		inputScanner: inputScanner,
	}, nil
}

func (s *InputFileImpl) Close() error {
	return s.inputFile.Close()
}

func (s *InputFileImpl) Scan() bool {
	return s.inputScanner.Scan()
}

func (s *InputFileImpl) ReadLine() string {
	return s.inputScanner.Text()
}

func (s *InputFileImpl) ReadMappedLine() (string, int, error) {
	parts := strings.Split(s.ReadLine(), "\t")
	word := parts[0]
	count, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("second part in line should be integer, but was not. Error=%w", err)
	}

	return word, count, nil
}

func (s *InputFileImpl) Err() error {
	return s.inputScanner.Err()
}

type OutputFileImpl struct {
	file   *os.File
	writer *bufio.Writer
}

func newOutputFile(fileName string) (*OutputFileImpl, error) {
	file, err := os.Create(fileName)
	if err != nil {
		return nil, fmt.Errorf("newOutputFile filed, error=%w", err)
	}
	writer := bufio.NewWriter(file)

	return &OutputFileImpl{
			file:   file,
			writer: writer,
		},
		nil
}

func (s *OutputFileImpl) Close() {
	s.file.Close()
}

func (s *OutputFileImpl) WriteLine(line string) error {
	_, err := fmt.Println(s.writer, line)
	if err != nil {
		return fmt.Errorf("write to file filed, error=%w", err)
	}

	return nil
}

func (s *Service) Do(ctx context.Context, inputFileName string) error {
	tempFiles, err := s.MapAndShuffle(ctx, inputFileName)
	if err != nil {
		return fmt.Errorf("map and shuffle stage failed, error=%w", err)
	}

	outputFileName := s.reduce(tempFiles)
	os.Rename(outputFileName, "output.txt")

	return nil
}

func (s *Service) MapAndShuffle(ctx context.Context, inputFileName string) ([]string, error) {
	const (
		tmpFilePrefix    = "tmp"
		tmpFileExtension = "txt" // without dot!
	)

	inputFile, err := s.storage.OpenInputFile(inputFileName)
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
	tempFiles := []string{}

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

	return tempFiles, nil
}

func (s *Service) shuffleAndSendToWorker(ctx context.Context, wordCount map[string]int, fileIndex int) (string, error) {
	tempFileName := fmt.Sprintf("temp_%d.tsv", fileIndex)
	// file, err := os.Create(tempFileName)
	// if err != nil {
	// return "", fmt.Errorf("create temp file failed, error=%w", err)
	// }
	// defer file.Close()

	writer, err := s.storage.CreateOutputFile(tempFileName)
	if err != nil {
		return "", fmt.Errorf("create temp file failed, error=%w", err)
	}
	defer writer.Close()
	// writer := bufio.NewWriter(file)

	// Сортируем слова перед записью
	words := make([]string, 0, len(wordCount))
	for word := range wordCount {
		words = append(words, word)
	}
	sort.Strings(words) // Лексикографическая сортировка

	// Записываем в файл
	for _, word := range words {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("context cancelled, error if any=%w", ctx.Err())
		default:
		}
		line := fmt.Sprintf("%s\t%d", word, wordCount[word])
		err := writer.WriteLine(line)
		if err != nil {
			return "", fmt.Errorf("temp file write line failed, error=%w", err)
		}
	}

	return tempFileName, nil
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

func (s *Service) openReadFiles(tempFiles []string) ([]InputFile, error) {
	res := make([]InputFile, len(tempFiles))
	for i, f := range tempFiles {
		inF, err := s.storage.OpenInputFile(f)
		if err != nil {
			return nil, fmt.Errorf("Failed to open files in storage", zap.Error(err))
		}
		res[i] = inF
	}

	return res, nil
}

func (s *Service) mergeSortedFiles(tempFiles []string, outputFile string) error {
	files, err := s.openReadFiles(tempFiles)
	if err != nil {
		return fmt.Errorf("Failed to open files in storage", zap.Error(err))
	}
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()

	writer, err := s.storage.CreateOutputFile(outputFile)
	if err != nil {
		return fmt.Errorf("Failed to create output file in storage", zap.Error(err))
	}
	defer writer.Close()

	// Create min-heap of words
	minHeap := newWordHeap()

	for i, f := range files {
		if f.Scan() {
			word, count, err := f.ReadMappedLine()
			if err != nil {

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
				writer.WriteLine(line)
			}
			prevWord = entry.word
			totalCount = entry.count
		}

		// Читаем следующую строку из того же файла
		f := files[entry.fileIndex]
		if f.Scan() {
			//parts := strings.Split(scanners[entry.fileIndex].Text(), "\t")
			//count, _ := strconv.Atoi(parts[1])
			word, count, err := f.ReadMappedLine()
			if err != nil {
				return fmt.Errorf("ReadMappedLine failed, error=%w", err)
			}
			heap.Push(minHeap, WordEntry{word: word, count: count, fileIndex: entry.fileIndex})
		}

		// Записываем последнее слово
		if prevWord != "" {
			line := fmt.Sprintf("%s\t%d\n", prevWord, totalCount)
			err := writer.WriteLine(line)
			if err != nil {
				return fmt.Errorf("WriteLine failed, error=%w", err)
			}
		}
	}

	return nil
}

func (s *Service) reduce(tempFiles []string) string {
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
//}
