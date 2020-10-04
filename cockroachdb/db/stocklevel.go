package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach-go/crdb"
)

func (d *Driver) RunStockLevelTxn(warehouseID, districtID, threshold, lastOrderNum int) {
	fmt.Fprintln(os.Stdout, "[Stock-Level output]")
	total := 0
	// Transaction
	if err := crdb.ExecuteTx(context.Background(), d.db, nil, func(tx *sql.Tx) error {
		// Get next order id
		var n int
		if err := tx.QueryRow(
			"SELECT d_next_o_id FROM district WHERE d_w_id = $1 AND d_id = $2",
			warehouseID, districtID,
		).Scan(&n); err != nil {
			return err
		}
		// Get items of order
		rows, err := tx.Query(
			"SELECT ol_i_id FROM orderline WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id >= $3 AND ol_o_id < $4",
			warehouseID, districtID, n-lastOrderNum, n,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		items := make(map[int]struct{})
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				return err
			}
			items[id] = struct{}{}
		}
		// Compare with the threshold
		for id := range items {
			var quantity int
			if err := tx.QueryRow(
				"SELECT s_quantity FROM stock WHERE s_w_id = $1 AND s_i_id = $2",
				warehouseID, id,
			).Scan(&quantity); err != nil {
				return err
			}
			if quantity < threshold {
				total++
			}
		}
		return nil
	}); err != nil {
		fmt.Fprintln(os.Stderr, "run stock level txn failed:", err)
		return
	}
	// Output
	fmt.Fprintln(os.Stdout, total)
	fmt.Fprintln(os.Stdout, "[Stock-Level done]")
}
