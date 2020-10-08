package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/alecthomas/kingpin"

	"cs5424/cockroachdb/db"
)

var (
	userName    = kingpin.Flag("user", "user name").Default("root").String()
	database    = kingpin.Flag("database", "database name").Default("wholesale").String()
	endpointStr = kingpin.Flag("endpoints", "endpoint1,endpoint2,...").Required().String()
	clientNum   = kingpin.Flag("num", "client num").Required().Int()
	fileDir     = kingpin.Flag("dir", "file directory").Required().String()
	fileOutput  = kingpin.Flag("out-file", "output to file").Default("false").Bool()
)

func main() {
	kingpin.Parse()

	endpoints := strings.Split(*endpointStr, ",")
	wg := &sync.WaitGroup{}
	for i := 1; i <= *clientNum; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f, err := os.Open(filepath.Join(*fileDir, fmt.Sprintf("%v.txt", i)))
			if err != nil {
				log.Fatalf("open file %s failed: %v", i, err)
			}
			defer f.Close()
			var driver *db.Driver
			if *fileOutput {
				outFile, err := os.Open(filepath.Join(*fileDir, fmt.Sprintf("%v.out", i)))
				if err != nil {
					log.Fatalf("open output file %v failed: %v", i, err)
				}
				driver, err = db.NewDriver(*userName, endpoints[i%len(endpoints)], *database, f, outFile, outFile)
			} else {
				driver, err = db.NewDriver(*userName, endpoints[i%len(endpoints)], *database, f, os.Stdout, os.Stderr)
			}
			if err != nil {
				log.Fatalf("new db driver failed: %v", err)
			}
			defer driver.Stop()
			driver.Run()
		}(i)
	}
	wg.Wait()
}
