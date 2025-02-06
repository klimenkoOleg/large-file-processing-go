package main

import (
	"context"

	"go.uber.org/zap"

	"large-file-processing-go/internal/domain/mapreduce"
)

func main() {
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	n := 5
	workers := 1
	service := mapreduce.NewService(n, workers, l)
	err = service.Do(context.Background(), "input.txt")
	if err != nil {
		l.Error("service execution error", zap.Error(err))
	}
}
