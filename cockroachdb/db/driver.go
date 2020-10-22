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
	db     *sql.DB
	br     *bufio.Reader
	out    io.Writer
	errOut io.Writer
}

func NewDriver(user, endpoint, database string, r io.Reader, w io.Writer, errOut io.Writer) (*Driver, error) {
	db, err := sql.Open("postgres",
		fmt.Sprintf(dbSourceName, user, endpoint, database))
	if err != nil {
		return nil, err
	}
	return &Driver{
		db:  db,
		br:  bufio.NewReader(r),
		out: w,
		errOut: errOut,
	}, nil
}

func (d *Driver) Run() {
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
			txnTime = append(txnTime, d.RunNewOrderTxn(nums[0], nums[1], nums[2], nums[3]))
		case "P":
			nums, err := utils.StringsToFloats(paras[1:])
			if err != nil {
				log.Fatalf("parse float failed: %v", err)
			}
			txnTime = append(txnTime, d.RunPaymentTxn(int(nums[0]), int(nums[1]), int(nums[2]), nums[3]))
		case "D":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime = append(txnTime, d.RunDeliveryTxn(nums[0], nums[1]))
		case "O":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime = append(txnTime, d.RunOrderStatusTxn(nums[0], nums[1], nums[2]))
		case "S":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime = append(txnTime, d.RunStockLevelTxn(nums[0], nums[1], nums[2], nums[3]))
		case "I":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime = append(txnTime, d.RunPopularItemTxn(nums[0], nums[1], nums[2]))
		case "T":
			txnTime = append(txnTime, d.RunTopBalanceTxn())
		case "R":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime = append(txnTime, d.RunRelatedCustomerTxn(nums[0], nums[1], nums[2]))
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
	fmt.Fprintf(d.out, "Total number of transactions processed: %v\n", len(txnTime))
	fmt.Fprintf(d.out, "Total elapsed time for processing the transactions: %vs\n", totalElapsed.Seconds())
	fmt.Fprintf(d.out, "Transaction throughput: %vtxn/s\n", float64(len(txnTime))/totalElapsed.Seconds())
	fmt.Fprintf(d.out, "Average transaction latency: %vms\n", float64(totalElapsed.Milliseconds())/float64(len(txnTime)))
	fmt.Fprintf(d.out, "Median transaction latency: %vms\n", txnTime[len(txnTime)/2].Milliseconds())
	fmt.Fprintf(d.out, "95th percentile transaction latency: %vms\n", txnTime[int(float64(len(txnTime))*0.95)].Milliseconds())
	fmt.Fprintf(d.out, "99th percentile transaction latency: %vms\n", txnTime[int(float64(len(txnTime))*0.99)].Milliseconds())
}

func (d *Driver) Stop() error {
	return d.db.Close()
}
