package main

import (
	"context"
	"log"
	"os"

	fileAdapter "github.com/klimenkoOleg/large-file-processing-go/internal/adapter/file"
	"github.com/klimenkoOleg/large-file-processing-go/internal/domain/mapreduce"
)

func main() {
	n := 2
	workers := 1
	storage := fileAdapter.NewStorage()
	service := mapreduce.NewService(n, workers, storage)
	outputFileName, err := service.Do(context.Background(), "input.txt")
	if err != nil {
		log.Fatal(err)
	}

	err = os.Rename(outputFileName, "output.tsv")
	if err != nil {
		log.Fatal(err)
	}
}
