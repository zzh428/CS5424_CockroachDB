package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
)

type popularOrderDetail struct {
	date          sql.NullTime
	customerID    int
	firstName     string
	middleName    string
	lastName      string
	maxQuantity   int
	item2Quantity map[int]int
}

type popularItemDetail struct {
	name     string
	orderSet map[int]struct{}
}

func (d *Driver) RunPopularItemTxn(warehouseID, districtID, lastOrderNum int) time.Duration {
	fmt.Fprintln(d.out, "[Popular-Item output]")
	popularItems := make(map[int]*popularItemDetail)
	allOrder := make(map[int]*popularOrderDetail)
	// Transaction
	start := time.Now()
	if err := crdb.ExecuteTx(context.Background(), d.db, nil, func(tx *sql.Tx) error {
		// Get next order id
		var n int
		if err := tx.QueryRow(
			"SELECT d_next_o_id FROM district WHERE d_w_id = $1 AND d_id = $2",
			warehouseID, districtID,
		).Scan(&n); err != nil {
			return err
		}
		// Get last orders
		rows, err := tx.Query(
			"SELECT o_id, o_c_id, o_entry_d FROM orders WHERE o_w_id = $1 AND o_d_id = $2 AND o_id >= $3 AND o_id < $4",
			warehouseID, districtID, n-lastOrderNum, n,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				orderID    int
				customerID int
				date       sql.NullTime
			)
			if err := rows.Scan(&orderID, &customerID, &date); err != nil {
				return err
			}
			allOrder[orderID] = &popularOrderDetail{
				customerID:    customerID,
				date:          date,
				item2Quantity: make(map[int]int),
			}
		}
		// Get customer detail
		for _, p := range allOrder {
			if err := tx.QueryRow(
				"SELECT c_first, c_middle, c_last FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3",
				warehouseID, districtID, p.customerID,
			).Scan(&p.firstName, &p.middleName, &p.lastName); err != nil {
				return err
			}
		}
		// Deal with each order
		for orderID, p := range allOrder {
			// Get order lines of this order
			orderLineRows, err := tx.Query(
				"SELECT ol_i_id, ol_quantity FROM orderline WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3",
				warehouseID, districtID, orderID,
			)
			if err != nil {
				return err
			}
			for orderLineRows.Next() {
				var (
					itemID   int
					quantity int
				)
				if err := orderLineRows.Scan(&itemID, &quantity); err != nil {
					return err
				}
				if p, ok := popularItems[itemID]; ok {
					p.orderSet[orderID] = struct{}{}
				}
				p.item2Quantity[itemID] += quantity
				if p.item2Quantity[itemID] > p.maxQuantity {
					p.maxQuantity = p.item2Quantity[itemID]
				}
			}
			orderLineRows.Close()
			// Find popular items
			for itemID, quantity := range p.item2Quantity {
				if quantity < p.maxQuantity {
					continue
				}
				if _, ok := popularItems[itemID]; !ok {
					var name string
					if err := tx.QueryRow("SELECT i_name FROM item WHERE i_id = $1", itemID).Scan(&name); err != nil {
						return err
					}
					popularItems[itemID] = &popularItemDetail{
						name:     name,
						orderSet: map[int]struct{}{orderID: {}},
					}
				}
			}
		}
		return nil
	}); err != nil {
		fmt.Fprintln(d.errOut, "run popular item txn failed:", err)
		return 0
	}
	duration := time.Since(start)
	// Output
	fmt.Fprintln(d.out, warehouseID, districtID)
	fmt.Fprintln(d.out, lastOrderNum)
	for orderID, p := range allOrder {
		var orderDate string
		if p.date.Valid {
			orderDate = p.date.Time.Format("2006/01/02 15:04:05 PM")
		} else {
			orderDate = "NULL"
		}
		fmt.Fprintln(d.out, orderID, orderDate)
		fmt.Fprintln(d.out, p.firstName, p.middleName, p.lastName)
		for itemID, quantity := range p.item2Quantity {
			if quantity >= p.maxQuantity {
				fmt.Fprintln(d.out, popularItems[itemID].name, quantity)
			}
		}
	}
	for _, p := range popularItems {
		fmt.Fprintln(d.out,
			p.name, fmt.Sprintf("%.2f%%", 100*float64(len(p.orderSet))/float64(len(allOrder))),
		)
	}
	fmt.Fprintln(d.out, "[Popular-Item done]")
	return duration
}
