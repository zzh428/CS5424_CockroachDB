package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"

	"cs5424/cockroachdb/utils"
)

type newOrderItem struct {
	id          int
	warehouseID int
	quantity    int
	name        string
	amount      float64
	sQuantity   int
}

type newOrderOutput struct {
	cLast, cCredit        string
	wTax, dTax, cDiscount float64
	orderID               int
	totalAmount           float64
	entryDate             time.Time
}

func (d *Driver) RunNewOrderTxn(customerID, warehouseID, districtID, itemNum int) time.Duration {
	items, err := d.getNewOrderItems(itemNum)
	if err != nil {
		fmt.Fprintln(d.errOut, "get new order items failed", err)
		return 0
	}
	fmt.Fprintln(d.out, "[New-Order output]")
	allLocal := 1
	for _, item := range items {
		if item.warehouseID != warehouseID {
			allLocal = 0
			break
		}
	}
	var output newOrderOutput
	// Transaction
	start := time.Now()
	if err := crdb.ExecuteTx(context.Background(), d.db, nil, func(tx *sql.Tx) error {
		// Get next order id
		if err := tx.QueryRow(
			"SELECT d_next_o_id FROM district WHERE d_w_id = $1 AND d_id = $2",
			warehouseID, districtID,
		).Scan(&output.orderID); err != nil {
			return err
		}
		// Increase d_next_o_id by one
		if _, err := tx.Exec(
			"UPDATE district SET d_next_o_id = $1 WHERE d_w_id = $2 AND d_id = $3",
			output.orderID+1, warehouseID, districtID,
		); err != nil {
			return err
		}
		// Create new order
		output.entryDate = time.Now()
		if _, err := tx.Exec(
			"INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local, o_carrier_id) VALUES ($1, $2, $3, $4, $5, $6, $7, NULL)",
			output.orderID, districtID, warehouseID, customerID, output.entryDate, itemNum, allLocal,
		); err != nil {
			return err
		}
		// Item operations
		for i := range items {
			// Update stock
			if err := tx.QueryRow(
				"SELECT s_quantity FROM stock WHERE s_w_id = $1 AND s_i_id = $2",
				items[i].warehouseID, items[i].id,
			).Scan(&items[i].sQuantity); err != nil {
				return err
			}
			adjustQty := items[i].sQuantity - items[i].quantity
			if adjustQty < 10 {
				adjustQty += 100
			}
			if items[i].warehouseID != warehouseID {
				if _, err := tx.Exec(
					"UPDATE stock SET s_quantity = $1, s_ytd = s_ytd + $2, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + 1 WHERE s_w_id = $3 AND s_i_id = $4",
					adjustQty, items[i].quantity, items[i].warehouseID, items[i].id,
				); err != nil {
					return err
				}
			} else {
				if _, err := tx.Exec(
					"UPDATE stock SET s_quantity = $1, s_ytd = s_ytd + $2, s_order_cnt = s_order_cnt + 1 WHERE s_w_id = $3 AND s_i_id = $4",
					adjustQty, items[i].quantity, items[i].warehouseID, items[i].id,
				); err != nil {
					return err
				}
			}
			// Create new order line
			var itemPrice float64
			if err := tx.QueryRow(
				"SELECT i_price, i_name FROM item WHERE i_id = $1",
				items[i].id,
			).Scan(&itemPrice, &items[i].name); err != nil {
				return err
			}
			items[i].amount = float64(items[i].quantity) * itemPrice
			output.totalAmount += items[i].amount
			if _, err := tx.Exec(
				"INSERT INTO orderline (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info, ol_delivery_d) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL)",
				output.orderID, districtID, warehouseID, i, items[i].id, items[i].warehouseID, items[i].quantity, items[i].amount, fmt.Sprintf("S_DIST_%v", districtID),
			); err != nil {
				return err
			}
		}
		if err := tx.QueryRow(
			"SELECT w_tax FROM warehouse WHERE w_id = $1",
			warehouseID,
		).Scan(&output.wTax); err != nil {
			return err
		}
		if err := tx.QueryRow(
			"SELECT d_tax FROM district WHERE d_w_id = $1 AND d_id = $2",
			warehouseID, districtID,
		).Scan(&output.dTax); err != nil {
			return err
		}
		if err := tx.QueryRow(
			"SELECT c_discount, c_last, c_credit FROM customer WHERE c_id = $1",
			customerID,
		).Scan(&output.cDiscount, &output.cLast, &output.cCredit); err != nil {
			return err
		}
		output.totalAmount *= (1 + output.dTax + output.wTax) * (1 - output.cDiscount)
		return nil
	}); err != nil {
		fmt.Fprintln(d.errOut, "run new order txn failed:", err)
		return 0
	}
	duration := time.Since(start)
	// Output
	fmt.Fprintln(d.out, warehouseID, districtID, customerID, output.cLast, output.cCredit, output.cDiscount)
	fmt.Fprintln(d.out, output.wTax, output.dTax)
	fmt.Fprintln(d.out, output.orderID, output.entryDate.Format("2006/01/02 15:04:05 PM"))
	fmt.Fprintln(d.out, itemNum, output.totalAmount)
	for _, item := range items {
		fmt.Fprintln(d.out, item.id, item.name, item.warehouseID, item.quantity, item.amount, item.sQuantity)
	}
	fmt.Fprintln(d.out, "[New-Order done]")
	return duration
}

func (d *Driver) getNewOrderItems(num int) ([]*newOrderItem, error) {
	items := make([]*newOrderItem, num, num)
	for i := 0; i < num; i++ {
		line, _, err := d.br.ReadLine()
		if err != nil {
			return items, err
		}
		numsStr := strings.Split(string(line), ",")
		nums, err := utils.StringsToInts(numsStr)
		if err != nil {
			return items, err
		}
		items[i] = &newOrderItem{
			id:          nums[0],
			warehouseID: nums[1],
			quantity:    nums[2],
		}
	}
	return items, nil
}
