package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const totalFile = 3000
const contentLength = 5000
const numberOfWorkers = 100

var tempPath = filepath.Join("pipeline-temp")

type FileInfo struct {
	Index       int
	FileName    string
	WorkerIndex int
	Err         error
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	log.Println("start")
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	generateFiles(ctx)

	duration := time.Since(start)
	log.Println("done in", duration.Seconds(), "seconds")
}

func randomString(length int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	b := make([]rune, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

func generateFiles(ctx context.Context) {
	os.RemoveAll(tempPath)
	os.MkdirAll(tempPath, os.ModePerm)

	done := make(chan int)

	go func() {
		// pipeline 1: job distribution
		chanFileIndex := generateFileIndexes(ctx)

		// pipeline 2 creating files
		chanFileResult := createFiles(ctx, chanFileIndex, numberOfWorkers)

		counterSuccess := 0
		for fileResult := range chanFileResult {
			if fileResult.Err != nil {
				log.Printf("error creating file %s. stack trace: %s", fileResult.FileName, fileResult.Err)
			} else {
				counterSuccess++
			}
		}

		done <- counterSuccess
	}()

	select {
	case <-ctx.Done():
		log.Printf("generation process stopped. %s", ctx.Err())
	case counterSuccess := <-done:
		log.Printf("%d/%d of total files created", counterSuccess, totalFile)
	}
}

func generateFileIndexes(ctx context.Context) <-chan FileInfo {
	chanOut := make(chan FileInfo)
	go func() {
		defer close(chanOut)
		for i := 0; i < totalFile; i++ {
			select {
			case <-ctx.Done():
				break
			default:
				chanOut <- FileInfo{
					Index:    i,
					FileName: fmt.Sprintf("file-%d.txt", i),
				}
			}
		}
	}()

	return chanOut
}

func createFiles(ctx context.Context, chanIn <-chan FileInfo, numberOfWorkers int) <-chan FileInfo {
	chanOut := make(chan FileInfo)

	wg := new(sync.WaitGroup)
	wg.Add(numberOfWorkers)

	go func() {
		for workerIndex := 0; workerIndex < numberOfWorkers; workerIndex++ {

			go func(workerIndex int) {
				defer wg.Done()

				for job := range chanIn {
					select {
					case <-ctx.Done():
						break
					default:
						filePath := filepath.Join(tempPath, job.FileName)
						content := randomString(contentLength)
						err := ioutil.WriteFile(filePath, []byte(content), os.ModePerm)

						log.Println("worker", workerIndex, "working on", job.FileName, "file generator")

						job.WorkerIndex = workerIndex
						job.Err = err
						chanOut <- job
					}
				}
			}(workerIndex)
		}
	}()

	go func() {
		wg.Wait()
		close(chanOut)
	}()

	return chanOut
}
