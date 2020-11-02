package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
)

func (d *Driver) RunDeliveryTxn(db *sql.DB, warehouseID, carrierID int) time.Duration {
	fmt.Fprintln(d.out, "[Delivery output]")
	// Transaction
	start := time.Now()
	if err := crdb.ExecuteTx(context.Background(), db, nil, func(tx *sql.Tx) error {
		// District 1 to 10
		for districtID := 1; districtID <= 10; districtID++ {
			// Get smallest order number
			var orderIDNull sql.NullInt64
			if err := tx.QueryRow(
				"SELECT min(o_id) FROM orders WHERE o_w_id = $1 AND o_d_id = $2 AND o_carrier_id is NULL",
				warehouseID, districtID,
			).Scan(&orderIDNull); err != nil {
				return err
			}
			if !orderIDNull.Valid {
				fmt.Fprintln(d.out, "order ID is null: ", warehouseID, districtID)
				continue
			}
			orderID := orderIDNull.Int64
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
			var amountNull sql.NullFloat64
			if err := tx.QueryRow(
				"SELECT sum(ol_amount) FROM orderline WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3",
				warehouseID, districtID, orderID,
			).Scan(&amountNull); err != nil {
				return err
			}
			var amount float64
			if amountNull.Valid {
				amount = amountNull.Float64
			} else {
				amount = 0
			}
			var customerIDNull sql.NullInt64
			if err := tx.QueryRow(
				"SELECT o_c_id FROM orders WHERE o_w_id = $1 AND o_d_id = $2 AND o_id = $3",
				warehouseID, districtID, orderID,
			).Scan(&customerIDNull); err != nil {
				return err
			}
			if !customerIDNull.Valid {
				fmt.Fprintln(d.out, "customer ID is null: ", warehouseID, districtID)
				continue
			}
			customerID := customerIDNull.Int64
			if _, err := tx.Exec(
				"UPDATE customer SET c_balance = c_balance + $1, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4",
				amount, warehouseID, districtID, customerID,
			); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		fmt.Fprintln(d.errOut, "run delivery txn failed:", err)
		return 0
	}
	duration := time.Since(start)
	// Output
	fmt.Fprintln(d.out, "[Delivery done]")
	return duration
}
