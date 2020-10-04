package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach-go/crdb"
)

func (d *Driver) RunRelatedCustomerTxn(warehouseID, districtID, customerID int) {
	fmt.Fprintln(os.Stdout, "[Related-Customer output]")
	customers := make(map[int]struct{})
	if err := crdb.ExecuteTx(context.Background(), d.db, nil, func(tx *sql.Tx) error {
		orderItems := make(map[int]map[int]struct{})
		rows, err := tx.Query(
			"SELECT o_id FROM orders WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3",
			warehouseID, districtID, customerID,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var orderID int
			if err := rows.Scan(&orderID); err != nil {
				return err
			}
			orderItems[orderID] = make(map[int]struct{})
		}
		for orderID := range orderItems {
			rows, err := tx.Query(
				"SELECT ol_i_id FROM orderline WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3",
				warehouseID, districtID, orderID,
			)
			if err != nil {
				return err
			}
			for rows.Next() {
				var itemID int
				if err := rows.Scan(&itemID); err != nil {
					rows.Close()
					return err
				}
				orderItems[orderID][itemID] = struct{}{}
			}
			rows.Close()
		}
		type customerDetail struct {
			warehouseID, districtID int
			orderSet                map[int]struct{}
		}
		otherCustomerOrders := make(map[int]*customerDetail)
		rows, err = tx.Query(
			"SELECT c_w_id, c_d_id, c_id FROM customer WHERE c_w_id != $1",
			warehouseID,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				cID int
				c   customerDetail
			)
			if err := rows.Scan(&c.warehouseID, &c.districtID, &cID); err != nil {
				return err
			}
			otherCustomerOrders[cID] = &c
		}
		for cID, p := range otherCustomerOrders {
			rows, err := tx.Query(
				"SELECT o_id FROM orders WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3",
				p.warehouseID, p.districtID, cID,
			)
			if err != nil {
				return err
			}
			p.orderSet = make(map[int]struct{})
			for rows.Next() {
				var orderID int
				if err := rows.Scan(&orderID); err != nil {
					rows.Close()
					return err
				}
				p.orderSet[orderID] = struct{}{}
			}
			rows.Close()
		}
	LOOP:
		for cID, p := range otherCustomerOrders {
			for oID := range p.orderSet {
				rows, err := tx.Query(
					"SELECT ol_i_id FROM orderline WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3",
					p.warehouseID, p.districtID, oID,
				)
				if err != nil {
					return err
				}
				itermIDs := make(map[int]struct{})
				for rows.Next() {
					var itemID int
					if err := rows.Scan(&itemID); err != nil {
						rows.Close()
						return err
					}
					itermIDs[itemID] = struct{}{}
				}
				rows.Close()
				for _, itermSet := range orderItems {
					sameNum := 0
					for itermID := range itermIDs {
						if _, ok := itermSet[itermID]; ok {
							sameNum++
						}
					}
					if sameNum >= 2 {
						customers[cID] = struct{}{}
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
	fmt.Fprintln(os.Stdout, "WarehouseID", warehouseID, "DistrictID", districtID, "CustomerID", customerID)
	for c := range customers {
		fmt.Fprintln(os.Stdout, "Related CustomerID", c)
	}
	fmt.Fprintln(os.Stdout, "[Related-Customer done]")
}
