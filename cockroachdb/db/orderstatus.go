package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach-go/crdb"
)

type orderStatusOutput struct {
	cFirst, cMiddle, cLast string
	cBalance               float64
	orderID                int
	orderDate              sql.NullTime
	orderCarrierID         sql.NullInt64
}

type orderStatusItem struct {
	itemID, warehouseID, quantity int
	amount                        float64
	date                          sql.NullTime
}

func (d *Driver) RunOrderStatusTxn(warehouseID, districtID, customerID int) {
	fmt.Fprintln(os.Stdout, "[Order-Status output]")
	var out orderStatusOutput
	items := make([]orderStatusItem, 0)
	// Transaction
	if err := crdb.ExecuteTx(context.Background(), d.db, nil, func(tx *sql.Tx) error {
		// Get customer info
		if err := tx.QueryRow(
			"SELECT c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3",
			warehouseID, districtID, customerID,
		).Scan(&out.cFirst, &out.cMiddle, &out.cLast, &out.cBalance); err != nil {
			return err
		}
		// Get order info
		if err := tx.QueryRow(
			"SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3",
			warehouseID, districtID, customerID,
		).Scan(&out.orderID, &out.orderDate, &out.orderCarrierID); err != nil {
			return err
		}
		// Get item info
		rows, err := tx.Query(
			"SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM orderline WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3",
			warehouseID, districtID, out.orderID,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item orderStatusItem
			if err := rows.Scan(&item.itemID, &item.warehouseID, &item.quantity, &item.amount, &item.date); err != nil {
				return err
			}
			items = append(items, item)
		}
		return nil
	}); err != nil {
		fmt.Fprintln(os.Stderr, "run order status txn failed:", err)
		return
	}
	// Output
	var orderDate, orderCarrierID string
	if out.orderDate.Valid {
		orderDate = out.orderDate.Time.Format("2006/01/02 15:04:05 PM")
	} else {
		orderDate = "NULL"
	}
	if out.orderCarrierID.Valid {
		orderCarrierID = fmt.Sprintf("%v", out.orderCarrierID.Int64)
	} else {
		orderCarrierID = "NULL"
	}
	fmt.Fprintln(os.Stdout, out.cFirst, out.cMiddle, out.cLast, out.cBalance)
	fmt.Fprintln(os.Stdout, out.orderID, orderDate, orderCarrierID)
	for _, item := range items {
		var itemDate string
		if item.date.Valid {
			itemDate = item.date.Time.Format("2006/01/02 15:04:05 PM")
		} else {
			itemDate = "NULL"
		}
		fmt.Fprintln(os.Stdout, item.itemID, item.warehouseID, item.quantity, item.amount, itemDate)
	}
	fmt.Fprintln(os.Stdout, "[Order-Status done]")
}
