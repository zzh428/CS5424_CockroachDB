package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
)

func (d *Driver) RunDeliveryTxn(warehouseID, carrierID int) {
	fmt.Fprintln(os.Stdout, "[Delivery output]")
	// Transaction
	if err := crdb.ExecuteTx(context.Background(), d.db, nil, func(tx *sql.Tx) error {
		// District 1 to 10
		for districtID := 1; districtID <= 10; districtID++ {
			// Get smallest order number
			var orderID int
			if err := tx.QueryRow(
				"SELECT min(o_id) FROM orders WHERE o_w_id = $1 AND o_d_id = $2 AND o_carrier_id is NULL",
				warehouseID, districtID,
			).Scan(&orderID); err != nil {
				return err
			}
			// Update order
			if _, err := tx.Exec(
				"UPDATE orders SET o_carrier_id = $1 WHERE o_w_id = $2 AND o_d_id = $3 AND o_id = $4",
				carrierID, warehouseID, districtID, orderID,
			); err != nil {
				return err
			}
			// Update order line
			if _, err := tx.Exec(
				"UPDATE orderline SET ol_delivery_d = $1 WHERE ol_w_id = $2 AND ol_d_id = $3 AND ol_o_id = $4",
				time.Now(), warehouseID, districtID, orderID,
			); err != nil {
				return err
			}
			//Update customer
			var amount float64
			if err := tx.QueryRow(
				"SELECT sum(ol_amount) FROM orderline WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3",
				warehouseID, districtID, orderID,
			).Scan(&amount); err != nil {
				return err
			}
			var customerID int
			if err := tx.QueryRow(
				"SELECT o_c_id FROM orders WHERE o_w_id = $1 AND o_d_id = $2 AND o_id = $3",
				warehouseID, districtID, orderID,
			).Scan(&customerID); err != nil {
				return err
			}
			if _, err := tx.Exec(
				"UPDATE customer SET c_balance = c_balance + $1, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4",
				amount, warehouseID, districtID, customerID,
			); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		fmt.Fprintln(os.Stderr, "run delivery txn failed:", err)
		return
	}
	// Output
	fmt.Fprintln(os.Stdout, "[Delivery done]")
}
