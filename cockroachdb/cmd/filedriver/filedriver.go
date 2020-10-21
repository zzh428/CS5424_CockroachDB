package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/alecthomas/kingpin"

	"cs5424/cockroachdb/db"
)

var (
	userName    = kingpin.Flag("user", "user name").Default("root").String()
	database    = kingpin.Flag("database", "database name").Default("wholesale").String()
	endpointStr = kingpin.Flag("endpoint", "endpoint of local node").Required().String()
	serverNum   = kingpin.Flag("server-num", "total number of all server node").Default("5").Int()
	serverSeq   = kingpin.Flag("server-seq", "sequence of local server, start from 0").Required().Int()
	txnFileNum  = kingpin.Flag("txn-file-num", "20 or 40").Required().Int()
	fileDir     = kingpin.Flag("dir", "file directory").Required().String()
	fileOutput  = kingpin.Flag("out-file", "output to file").Default("false").Bool()
)

func main() {
	kingpin.Parse()

	wg := &sync.WaitGroup{}
	for i := *serverSeq; i <= *txnFileNum; i += *serverNum {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f, err := os.Open(filepath.Join(*fileDir, fmt.Sprintf("%v.txt", i)))
			if err != nil {
				log.Fatalf("open file %v failed: %v", i, err)
			}
			defer f.Close()
			var driver *db.Driver
			if *fileOutput {
				var outFile *os.File
				outFile, err = os.Create(filepath.Join(*fileDir, fmt.Sprintf("%v.out", i)))
				if err != nil {
					log.Fatalf("create output file %v failed: %v", i, err)
				}
				defer outFile.Close()
				driver, err = db.NewDriver(*userName, *endpointStr, *database, f, outFile, outFile)
			} else {
				driver, err = db.NewDriver(*userName, *endpointStr, *database, f, os.Stdout, os.Stderr)
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
