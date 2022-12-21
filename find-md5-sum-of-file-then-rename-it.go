package main

import (
	"crypto/md5"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"
)

var tempPath = filepath.Join("pipeline-temp")

func main() {
	log.Println("start")
	start := time.Now()

	proceed()

	duration := time.Since(start)
	log.Println("done in", duration.Seconds(), "seconds")
}

func proceed() {
	counterTotal := 0
	counterRenamed := 0
	err := filepath.Walk(tempPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		counterTotal++

		buf, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		sum := fmt.Sprintf("%x", md5.Sum(buf))

		destinationPath := filepath.Join(tempPath, fmt.Sprintf("file-%s.txt", sum))
		//path "PROJECT/pipeline-temp/file-1.txt"
		//destinationPath "PROJECT/pipeline-temp/file-1782367812638761238726187.txt"

		err = os.Rename(path, destinationPath)
		if err != nil {
			return err
		}

		counterRenamed++
		return nil
	})

	if err != nil {
		log.Println("ERROR:", err.Error())
	}

	log.Printf("%d/%d file renamed", counterRenamed, counterTotal)
}
