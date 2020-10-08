package db

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
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
	}, nil
}

func (d *Driver) Run() {
	txnTime := make([][]time.Duration, 0)
	for i := 0; i < 8; i++ {
		txnTime = append(txnTime, make([]time.Duration, 0))
	}
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
			txnTime[0] = append(txnTime[0], d.RunNewOrderTxn(nums[0], nums[1], nums[2], nums[3]))
		case "P":
			nums, err := utils.StringsToFloats(paras[1:])
			if err != nil {
				log.Fatalf("parse float failed: %v", err)
			}
			txnTime[1] = append(txnTime[1], d.RunPaymentTxn(int(nums[0]), int(nums[1]), int(nums[2]), nums[3]))
		case "D":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime[2] = append(txnTime[2], d.RunDeliveryTxn(nums[0], nums[1]))
		case "O":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime[3] = append(txnTime[3], d.RunOrderStatusTxn(nums[0], nums[1], nums[2]))
		case "S":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime[4] = append(txnTime[4], d.RunStockLevelTxn(nums[0], nums[1], nums[2], nums[3]))
		case "I":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime[5] = append(txnTime[5], d.RunPopularItemTxn(nums[0], nums[1], nums[2]))
		case "T":
			txnTime[6] = append(txnTime[6], d.RunTopBalanceTxn())
		case "R":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			txnTime[7] = append(txnTime[7], d.RunRelatedCustomerTxn(nums[0], nums[1], nums[2]))
		default:
			fmt.Printf("invalid transaction: %s\n", paras[0])
		}
	}
	//TODO: Output the following
	// Total number of transactions processed
	// Total elapsed time for processing the transactions (in seconds)
	// Transaction throughput (number of transactions processed per second)
	// Average transaction latency (in ms)
	// Median transaction latency (in ms)
	// 95th percentile transaction latency (in ms)
	// 99th percentile transaction latency (in ms)
}

func (d *Driver) Stop() error {
	return d.db.Close()
}
