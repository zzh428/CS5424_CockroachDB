package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach-go/crdb"
)

type relatedCustomerInfo struct {
	warehouseID, districtID, customerID int
}

func (d *Driver) RunRelatedCustomerTxn(warehouseID, districtID, customerID int) {
	fmt.Fprintln(os.Stdout, "[Related-Customer output]")
	customers := make(map[int]struct{})
	// Transaction
	if err := crdb.ExecuteTx(context.Background(), d.db, nil, func(tx *sql.Tx) error {
		// Get customer's all order items
		orderItems := make(map[int]map[int]struct{})
		rows, err := tx.Query(
			"SELECT ol_o_id, ol_i_id FROM orderline WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id IN (SELECT o_id FROM orders WHERE o_w_id = $3 AND o_d_id = $4 AND o_c_id = $5)",
			warehouseID, districtID, warehouseID, districtID, customerID,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var orderID, itemID int
			if err := rows.Scan(&orderID, &itemID); err != nil {
				return err
			}
			if _, ok := orderItems[orderID]; !ok {
				orderItems[orderID] = make(map[int]struct{})
			}
			orderItems[orderID][itemID] = struct{}{}
		}
		// Get raw customers
		relatedCustomers := make([]relatedCustomerInfo, 0)
		rows, err = tx.Query(
			"SELECT c_w_id, c_d_id, c_id FROM customer WHERE c_w_id != $1",
			warehouseID,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var info relatedCustomerInfo
			if err := rows.Scan(&info.warehouseID, &info.districtID, &info.customerID); err != nil {
				return err
			}
			relatedCustomers = append(relatedCustomers, info)
		}
		// Find related
	LOOP:
		for _, info := range relatedCustomers {
			relatedOrderItems := make(map[int]map[int]struct{})
			rows, err := tx.Query(
				"SELECT ol_o_id, ol_i_id FROM orderline WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id IN (SELECT o_id FROM orders WHERE o_w_id = $3 AND o_d_id = $4 AND o_c_id = $5)",
				info.warehouseID, info.districtID, info.warehouseID, info.districtID, info.customerID,
			)
			if err != nil {
				return err
			}
			for rows.Next() {
				var orderID, itemID int
				if err := rows.Scan(&orderID, &itemID); err != nil {
					return err
				}
				if _, ok := relatedOrderItems[orderID]; !ok {
					relatedOrderItems[orderID] = make(map[int]struct{})
				}
				relatedOrderItems[orderID][itemID] = struct{}{}
			}
			rows.Close()
			for _, relatedItemSet := range relatedOrderItems {
				for _, itermSet := range orderItems {
					sameNum := 0
					for itermID := range relatedItemSet {
						if _, ok := itermSet[itermID]; ok {
							sameNum++
						}
					}
					if sameNum >= 2 {
						customers[info.customerID] = struct{}{}
						continue LOOP
					}
				}
			}
		}
		return nil
	}); err != nil {
		fmt.Fprintln(os.Stderr, "run related customer txn failed:", err)
		return
	}
	// Output
	fmt.Fprintln(os.Stdout, warehouseID, districtID, customerID)
	for c := range customers {
		fmt.Fprintln(os.Stdout, c)
	}
	fmt.Fprintln(os.Stdout, "[Related-Customer done]")
}
