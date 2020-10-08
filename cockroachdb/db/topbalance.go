package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
)

type topBalanceCustomer struct {
	warehouseID, districtID     int
	warehouseName, districtName string
	balance                     float64
	first, middle, last         string
}

func (d *Driver) RunTopBalanceTxn() time.Duration {
	fmt.Fprintln(d.out, "[Top-Balance output]")
	topTenCustomers := make([]*topBalanceCustomer, 0)
	// Transaction
	start := time.Now()
	if err := crdb.ExecuteTx(context.Background(), d.db, nil, func(tx *sql.Tx) error {
		// Get top 10 balance customers
		rows, err := tx.Query("SELECT c_first, c_middle, c_last, c_balance, c_w_id, c_d_id FROM customer ORDER BY c_balance DESC LIMIT 10")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c topBalanceCustomer
			if err := rows.Scan(&c.first, &c.middle, &c.last, &c.balance, &c.warehouseID, &c.districtID); err != nil {
				return err
			}
			topTenCustomers = append(topTenCustomers, &c)
		}
		// Get customer's warehouse info
		for _, c := range topTenCustomers {
			if err := tx.QueryRow(
				"SELECT w_name FROM warehouse WHERE w_id = $1",
				c.warehouseID,
			).Scan(&c.warehouseName); err != nil {
				return err
			}
			if err := tx.QueryRow(
				"SELECT d_name FROM district WHERE d_w_id = $1 AND d_id = $2",
				c.warehouseID, c.districtID,
			).Scan(&c.districtName); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		fmt.Fprintln(d.errOut, "run top balance txn failed:", err)
		return 0
	}
	duration := time.Since(start)
	// Output
	for _, c := range topTenCustomers {
		fmt.Fprintln(d.out, c.first, c.middle, c.last)
		fmt.Fprintln(d.out, c.balance)
		fmt.Fprintln(d.out, c.warehouseName)
		fmt.Fprintln(d.out, c.districtName)
	}
	fmt.Fprintln(d.out, "[Top-Balance done]")
	return duration
}
