package db

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"

	_ "github.com/lib/pq"

	"cs5424/cockroachdb/utils"
)

const dbSourceName = "postgresql://%s@%s/%s?sslmode=disable"

type Driver struct {
	db *sql.DB
	br *bufio.Reader
}

func NewDriver(user, endpoint, database string, r io.Reader) (*Driver, error) {
	db, err := sql.Open("postgres",
		fmt.Sprintf(dbSourceName, user, endpoint, database))
	if err != nil {
		return nil, err
	}
	return &Driver{
		db: db,
		br: bufio.NewReader(r),
	}, nil
}

func (d *Driver) Run() {
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
			d.RunNewOrderTxn(nums[0], nums[1], nums[2], nums[3])
		case "P":
			nums, err := utils.StringsToFloats(paras[1:])
			if err != nil {
				log.Fatalf("parse float failed: %v", err)
			}
			d.RunPaymentTxn(int(nums[0]), int(nums[1]), int(nums[2]), nums[3])
		case "D":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			d.RunDeliveryTxn(nums[0], nums[1])
		case "O":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			d.RunOrderStatusTxn(nums[0], nums[1], nums[2])
		case "S":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			d.RunStockLevelTxn(nums[0], nums[1], nums[2], nums[3])
		case "I":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			d.RunPopularItemTxn(nums[0], nums[1], nums[2])
		case "T":
			d.RunTopBalanceTxn()
		case "R":
			nums, err := utils.StringsToInts(paras[1:])
			if err != nil {
				log.Fatalf("parse int failed: %v", err)
			}
			d.RunRelatedCustomerTxn(nums[0], nums[1], nums[2])
		default:
			fmt.Printf("invalid transaction: %s\n", paras[0])
		}
	}
}

func (d *Driver) Stop() error {
	return d.db.Close()
}
