package file

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	mapReduceDomain "github.com/klimenkoOleg/large-file-processing-go/internal/domain/mapreduce"
)

type StorageImpl struct {
}

func NewStorage() *StorageImpl {
	return &StorageImpl{}
}

func (s *StorageImpl) OpenInputFile(name string) (mapReduceDomain.InputFile, error) {
	return newInputFile(name)
}

func (s *StorageImpl) CreateOutputFile(name string) (mapReduceDomain.OutputFile, error) {
	return newOutputFile(name)
}

type InputFileImpl struct {
	inputFile    *os.File
	inputScanner *bufio.Scanner
}

func newInputFile(name string) (*InputFileImpl, error) {
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

func (s *OutputFileImpl) Close() error {
	err := s.writer.Flush()
	if err != nil {
		return err
	}
	err = s.file.Close()
	if err != nil {
		return err
	}

	return nil
}

func (s *OutputFileImpl) Write(line string) error {
	_, err := s.writer.Write([]byte(line))
	if err != nil {
		return fmt.Errorf("write to file filed, error=%w", err)
	}

	return nil
}
