package main

import (
	"encoding/csv"
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
	serverSeq   = kingpin.Flag("server-seq", "sequence of local server, start from 1").Required().Int()
	txnFileNum  = kingpin.Flag("txn-file-num", "20 or 40").Required().Int()
	fileDir     = kingpin.Flag("dir", "file directory").Required().String()
	fileOutput  = kingpin.Flag("out-file", "output to file").Default("false").Bool()
)

func main() {
	kingpin.Parse()

	var measurements sync.Map

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
			measurements.Store(i, driver.Run())
		}(i)
	}
	wg.Wait()

	var experimentNum string
	switch *txnFileNum + *serverNum {
	case 24:
		experimentNum = "5"
	case 25:
		experimentNum = "6"
	case 44:
		experimentNum = "7"
	case 45:
		experimentNum = "8"
	default:
		log.Fatalf("invalid experiment: client_num[%v], server_num[%v]", *txnFileNum, *serverNum)
	}
	// Clients csv
	csvClients, err := os.Create(filepath.Join(*fileDir, fmt.Sprintf("clients-%s.csv", experimentNum)))
	if err != nil {
		log.Fatalf("create clients csv failed: %v", err)
	}
	defer csvClients.Close()

	cw := csv.NewWriter(csvClients)
	if err := cw.Write([]string{"experiment_number", "client_number", "measurement_a", "measurement_b", "measurement_c",
		"measurement_d", "measurement_e", "measurement_f", "measurement_g"}); err != nil {
		log.Fatalf("write clients csv failed: %v", err)
	}
	measurements.Range(func(key, value interface{}) bool {
		k, v := key.(int), value.(db.ClientMeasurement)
		if err := cw.Write([]string{experimentNum, fmt.Sprintf("%v", k), fmt.Sprintf("%v", v.TxnNum), fmt.Sprintf("%.2f", v.TotalSeconds), fmt.Sprintf("%.2f", v.Throughput),
			fmt.Sprintf("%.2f", v.AverageLatency), fmt.Sprintf("%v", v.MedianLatency), fmt.Sprintf("%v", v.Latency95), fmt.Sprintf("%v", v.Latency99)}); err != nil {
			log.Fatalf("write clients csv failed: %v", err)
		}
		return true
	})

	cw.Flush()
	if err := cw.Error(); err != nil {
		log.Fatalf("flush clients csv failed: %v", err)
	}
}
