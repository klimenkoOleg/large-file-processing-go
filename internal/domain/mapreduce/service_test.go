package mapreduce_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/klimenkoOleg/large-file-processing-go/internal/domain/mapreduce"
	mapReduceMocks "github.com/klimenkoOleg/large-file-processing-go/internal/domain/mapreduce/mocks"
)

func TestDo(t *testing.T) {
	mockStorage := new(mapReduceMocks.Storage)
	mockInputFile := new(mapReduceMocks.InputFile)
	mockOutputFile := new(mapReduceMocks.OutputFile)

	mockStorage.On("OpenInputFile", "input.txt").Return(mockInputFile, nil)
	mockStorage.On("CreateOutputFile", mock.Anything).Return(mockOutputFile, nil)
	mockInputFile.On("Close").Return(nil)
	mockOutputFile.On("Close").Return(nil)
	mockOutputFile.On("Write", mock.Anything).Return(nil)

	mockInputFile.On("Scan").Return(true).Once()
	mockInputFile.On("Scan").Return(false).Once()

	mockInputFile.On("ReadLine").Return("test_line").Once()

	service := mapreduce.NewService(10, 2, mockStorage)
	ctx := context.Background()

	outputFileName, err := service.Do(ctx, "input.txt")
	assert.NoError(t, err)

	mockStorage.AssertExpectations(t)
	mockInputFile.AssertExpectations(t)
	mockOutputFile.AssertExpectations(t)

	assert.Equal(t, "temp_0.tsv", outputFileName)
}

func TestService_Do_FailOpenInput(t *testing.T) {
	mockStorage := new(mapReduceMocks.Storage)
	svc := mapreduce.NewService(5, 2, mockStorage)
	ctx := context.Background()

	mockStorage.On("OpenInputFile", "input.txt").Return(nil, errors.New("file not found"))

	_, err := svc.Do(ctx, "input.txt")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "open input file failed")
}

func TestService_MapAndShuffle_EmptyFile(t *testing.T) {
	mockStorage := new(mapReduceMocks.Storage)
	mockInput := new(mapReduceMocks.InputFile)
	svc := mapreduce.NewService(5, 2, mockStorage)
	ctx := context.Background()

	mockStorage.On("OpenInputFile", "input.txt").Return(mockInput, nil)
	mockInput.On("Scan").Return(false) // No content in the file
	mockInput.On("Close").Return(nil)

	tempFiles, err := svc.MapAndShuffle(ctx, "input.txt")
	assert.NoError(t, err)
	assert.Empty(t, tempFiles)
}

func TestService_MapAndShuffle_ValidFile(t *testing.T) {
	mockStorage := new(mapReduceMocks.Storage)
	mockInput := new(mapReduceMocks.InputFile)
	mockOutput := new(mapReduceMocks.OutputFile)
	svc := mapreduce.NewService(2, 1, mockStorage)
	ctx := context.Background()

	mockStorage.On("OpenInputFile", "input.txt").Return(mockInput, nil)
	mockInput.On("Scan").Return(true).Once()
	mockInput.On("ReadLine").Return("word1").Once()
	mockInput.On("Scan").Return(true).Once()
	mockInput.On("ReadLine").Return("word2").Once()
	mockInput.On("Scan").Return(false).Once()
	mockInput.On("Close").Return(nil)

	mockStorage.On("CreateOutputFile", "temp_0.tsv").Return(mockOutput, nil)
	mockOutput.On("Write", mock.Anything).Return(nil)
	mockOutput.On("Close").Return(nil)

	tempFiles, err := svc.MapAndShuffle(ctx, "input.txt")
	assert.NoError(t, err)
	assert.Len(t, tempFiles, 1)
}
