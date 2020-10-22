package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/alecthomas/kingpin"
	_ "github.com/lib/pq"
)

const dbSourceName = "postgresql://%s@%s/%s?sslmode=disable"

var (
	userName      = kingpin.Flag("user", "user name").Default("root").String()
	database      = kingpin.Flag("database", "database name").Default("wholesale").String()
	endpointStr   = kingpin.Flag("endpoint", "endpoint of local node").Required().String()
	outDir        = kingpin.Flag("dir", "output dir").Required().String()
	experimentNum = kingpin.Flag("exp-num", "experiment number").Required().Enum("5", "6", "7", "8")
)

func main() {
	kingpin.Parse()

	db, err := sql.Open("postgres",
		fmt.Sprintf(dbSourceName, *userName, *endpointStr, *database))
	if err != nil {
		log.Fatalf("open database failed: %v", err)
	}
	defer db.Close()

	f, err := os.Create(filepath.Join(*outDir, fmt.Sprintf("db-state-%v.csv", *experimentNum)))
	if err != nil {
		log.Fatalf("create db state csv failed: %v", err)
	}
	defer f.Close()

	cw := csv.NewWriter(f)
	baseLine := []string{"experiment_number"}
	for i := 1; i <= 15; i++ {
		baseLine = append(baseLine, fmt.Sprintf("statistic_%v", i))
	}
	if err := cw.Write(baseLine); err != nil {
		log.Fatalf("write db state csv failed: %v", err)
	}

	resLine := make([]string, 16)
	resLine[0] = *experimentNum
	if err := db.QueryRow("SELECT sum(w_ytd) FROM warehouse").
		Scan(&resLine[1]); err != nil {
		log.Fatalf("query failed: %v", err)
	}
	if err := db.QueryRow("SELECT sum(d_ytd), sum(d_next_o_id) FROM district").
		Scan(&resLine[2], &resLine[3]); err != nil {
		log.Fatalf("query failed: %v", err)
	}
	if err := db.QueryRow("SELECT sum(c_balance), sum(c_ytd_payment), sum(c_payment_cnt), sum(c_delivery_cnt) FROM customer").
		Scan(&resLine[4], &resLine[5], &resLine[6], &resLine[7]); err != nil {
		log.Fatalf("query failed: %v", err)
	}
	if err := db.QueryRow("SELECT max(o_id), sum(o_ol_cnt) FROM orders").
		Scan(&resLine[8], &resLine[9]); err != nil {
		log.Fatalf("query failed: %v", err)
	}
	if err := db.QueryRow("SELECT sum(ol_amount), sum(ol_quantity) FROM orderline").
		Scan(&resLine[10], &resLine[11]); err != nil {
		log.Fatalf("query failed: %v", err)
	}
	if err := db.QueryRow("SELECT sum(s_quantity), sum(s_ytd), sum(s_order_cnt), sum(s_remote_cnt) FROM stock").
		Scan(&resLine[12], &resLine[13], &resLine[14], &resLine[15]); err != nil {
		log.Fatalf("query failed: %v", err)
	}
	if err := cw.Write(resLine); err != nil {
		log.Fatalf("write db state csv failed: %v", err)
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		log.Fatalf("flush db state csv failed: %v", err)
	}
}
