package db

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"cs5424/cockroachdb/utils"
)

const dbSourceName = "postgresql://%s@%s/%s?sslmode=disable"

type Driver struct {
	dbs    []*sql.DB
	br     *bufio.Reader
	out    io.Writer
	errOut io.Writer
}

type ClientMeasurement struct {
	TxnNum         int
	TotalSeconds   float64
	Throughput     float64
	AverageLatency float64
	MedianLatency  int64
	Latency95      int64
	Latency99      int64
}

func NewDriver(user, database string, endpoints []string, r io.Reader, w io.Writer, errOut io.Writer) (*Driver, error) {
	dbs := make([]*sql.DB, 0)
	for _, endpoint := range endpoints {
		db, err := sql.Open("postgres",
			fmt.Sprintf(dbSourceName, user, endpoint, database))
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, db)
	}
	return &Driver{
		dbs:    dbs,
		br:     bufio.NewReader(r),
		out:    w,
		errOut: errOut,
	}, nil
}

func (d *Driver) Run() ClientMeasurement {
	txnTime := make([]time.Duration, 0)
	for {
		line, _, err := d.br.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("read new line failed: %v", err)
		}
		if len(line) == 0 {
			continue
		}
		if string(line) == "EOF" {
			break
		}
		paras := strings.Split(string(line), ",")
		switch paras[0] {
		case "N":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime = append(txnTime, d.RunNewOrderTxn(d.dbs[len(txnTime)%len(d.dbs)], nums[0], nums[1], nums[2], nums[3]))
		case "P":
			nums, err := utils.StringsToFloats(paras[1:])
			if err != nil {
				log.Fatalf("parse float failed: %v", err)
			}
			txnTime = append(txnTime, d.RunPaymentTxn(d.dbs[len(txnTime)%len(d.dbs)], int(nums[0]), int(nums[1]), int(nums[2]), nums[3]))
		case "D":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime = append(txnTime, d.RunDeliveryTxn(d.dbs[len(txnTime)%len(d.dbs)], nums[0], nums[1]))
		case "O":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime = append(txnTime, d.RunOrderStatusTxn(d.dbs[len(txnTime)%len(d.dbs)], nums[0], nums[1], nums[2]))
		case "S":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime = append(txnTime, d.RunStockLevelTxn(d.dbs[len(txnTime)%len(d.dbs)], nums[0], nums[1], nums[2], nums[3]))
		case "I":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime = append(txnTime, d.RunPopularItemTxn(d.dbs[len(txnTime)%len(d.dbs)], nums[0], nums[1], nums[2]))
		case "T":
			txnTime = append(txnTime, d.RunTopBalanceTxn(d.dbs[len(txnTime)%len(d.dbs)]))
		case "R":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime = append(txnTime, d.RunRelatedCustomerTxn(d.dbs[len(txnTime)%len(d.dbs)], nums[0], nums[1], nums[2]))
		default:
			fmt.Printf("invalid transaction: %s\n", paras[0])
		}
	}
	// Output
	var totalElapsed time.Duration
	sort.Slice(txnTime, func(i, j int) bool {
		return txnTime[i] < txnTime[j]
	})
	for _, t := range txnTime {
		totalElapsed += t
	}
	cm := ClientMeasurement{
		TxnNum:         len(txnTime),
		TotalSeconds:   totalElapsed.Seconds(),
		Throughput:     float64(len(txnTime)) / totalElapsed.Seconds(),
		AverageLatency: float64(totalElapsed.Milliseconds()) / float64(len(txnTime)),
		MedianLatency:  txnTime[len(txnTime)/2].Milliseconds(),
		Latency95:      txnTime[int(float64(len(txnTime))*0.95)].Milliseconds(),
		Latency99:      txnTime[int(float64(len(txnTime))*0.99)].Milliseconds(),
	}

	fmt.Fprintf(d.out, "Total number of transactions processed: %v\n", cm.TxnNum)
	fmt.Fprintf(d.out, "Total elapsed time for processing the transactions: %.2fs\n", cm.TotalSeconds)
	fmt.Fprintf(d.out, "Transaction throughput: %.2ftxn/s\n", cm.Throughput)
	fmt.Fprintf(d.out, "Average transaction latency: %.2fms\n", cm.AverageLatency)
	fmt.Fprintf(d.out, "Median transaction latency: %vms\n", cm.MedianLatency)
	fmt.Fprintf(d.out, "95th percentile transaction latency: %vms\n", cm.Latency95)
	fmt.Fprintf(d.out, "99th percentile transaction latency: %vms\n", cm.Latency99)

	return cm
}

func (d *Driver) Stop() error {
	for _, db := range d.dbs {
		db.Close()
	}
	return nil
}
