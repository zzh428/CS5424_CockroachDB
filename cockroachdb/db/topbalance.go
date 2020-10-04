package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach-go/crdb"
)

type topBalanceCustomer struct {
	warehouseID, districtID     int
	warehouseName, districtName string
	balance                     float64
	first, middle, last         string
}

func (d *Driver) RunTopBalanceTxn() {
	fmt.Fprintln(os.Stdout, "[Top-Balance output]")
	topTenCustomers := make([]*topBalanceCustomer, 0)
	// Transaction
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
		fmt.Fprintln(os.Stderr, "run top balance txn failed:", err)
		return
	}
	// Output
	for _, c := range topTenCustomers {
		fmt.Fprintln(os.Stdout, c.first, c.middle, c.last)
		fmt.Fprintln(os.Stdout, c.balance)
		fmt.Fprintln(os.Stdout, c.warehouseName)
		fmt.Fprintln(os.Stdout, c.districtName)
	}
	fmt.Fprintln(os.Stdout, "[Top-Balance done]")
}
