package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
)

type relatedCustomerInfo struct {
	warehouseID, districtID, customerID int
}

func (d *Driver) RunRelatedCustomerTxn(warehouseID, districtID, customerID int) time.Duration {
	fmt.Fprintln(d.out, "[Related-Customer output]")
	relatedCustomers := make(map[relatedCustomerInfo]struct{})
	// Transaction
	start := time.Now()
	if err := crdb.ExecuteTx(context.Background(), d.db, nil, func(tx *sql.Tx) error {
		// Get customer's all order items
		itemOrderMap := make(map[int][]int)
		items := make(map[int]struct{})
		customerOrderMap := make(map[relatedCustomerInfo]map[int]map[int]struct{})
		rows, err := tx.Query(
			"SELECT ol_o_id, ol_i_id FROM orderline WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id IN (SELECT o_id FROM orders WHERE o_w_id = $3 AND o_d_id = $4 AND o_c_id = $5)",
			warehouseID, districtID, warehouseID, districtID, customerID,
		)
		if err != nil {
			return err
		}
		for rows.Next() {
			var orderID, itemID int
			if err := rows.Scan(&orderID, &itemID); err != nil {
				return err
			}
			if _, ok := itemOrderMap[itemID]; !ok {
				itemOrderMap[itemID] = make([]int, 0)
			}
			itemOrderMap[itemID] = append(itemOrderMap[itemID], orderID)
			items[itemID] = struct{}{}
		}

		rows.Close()
		// Get all orders that contain an item
		for item := range items {
			orders := itemOrderMap[item]
			rows, err = tx.Query(
				"SELECT ol_w_id, ol_d_id, ol_c_id, ol_o_id FROM orderline WHERE ol_w_id != $1 AND ol_i_id = $2",
				warehouseID, item,
			)
			if err != nil {
				return err
			}
			for rows.Next() {
				var info relatedCustomerInfo
				var orderID int
				if err := rows.Scan(&info.warehouseID, &info.districtID, &info.customerID, &orderID); err != nil {
					return err
				}
				if _, ok := relatedCustomers[info]; ok {
					continue
				}
				if _, ok := customerOrderMap[info]; !ok {
					customerOrderMap[info] = make(map[int]map[int]struct{})
				}
				if _, ok := customerOrderMap[info][orderID]; !ok {
					customerOrderMap[info][orderID] = make(map[int]struct{})
				}
				for _, o := range orders {
					if _, ok := customerOrderMap[info][orderID][o]; !ok {
						customerOrderMap[info][orderID][o] = struct{}{}
					} else {
						relatedCustomers[info] = struct{}{}
					}
				}
			}
			rows.Close()
		}
		return nil
	}); err != nil {
		fmt.Fprintln(d.out, "run related customer txn failed:", err)
		return 0
	}
	duration := time.Since(start)
	// Output
	fmt.Fprintln(d.out, warehouseID, districtID, customerID)
	for c := range relatedCustomers {
		fmt.Fprintln(d.out, c.warehouseID, c.districtID, c.customerID)
	}
	fmt.Fprintln(d.out, "[Related-Customer done]")
	return duration
}
