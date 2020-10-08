package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
)

type paymentOutput struct {
	cFirst, cMiddle, cLast                                           string
	cStreet1, cStreet2, cCity, cState, cZip, cPhone, cSince, cCredit string
	cCreditLimit, cDiscount, cBalance                                float64
	wStreet1, wStreet2, wCity, wState, wZip                          string
	dStreet1, dStreet2, dCity, dState, dZip                          string
}

func (d *Driver) RunPaymentTxn(warehouseID, districtID, customerID int, payment float64) time.Duration {
	fmt.Fprintln(d.out, "[Payment out]")
	var out paymentOutput
	// Transaction
	start := time.Now()
	if err := crdb.ExecuteTx(context.Background(), d.db, nil, func(tx *sql.Tx) error {
		// Update warehouse
		if _, err := tx.Exec(
			"UPDATE warehouse SET w_ytd = w_ytd + $1 WHERE w_id = $2",
			payment, warehouseID,
		); err != nil {
			return err
		}
		// Update district
		if _, err := tx.Exec(
			"UPDATE district SET d_ytd = d_ytd + $1 WHERE d_w_id = $2 AND d_id = $3",
			payment, warehouseID, districtID,
		); err != nil {
			return err
		}
		// Update customer
		if _, err := tx.Exec(
			"UPDATE customer SET c_balance = c_balance - $1, c_ytd_payment = c_ytd_payment + $2, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = $3 AND c_d_id = $4 AND c_id = $5",
			payment, payment, warehouseID, districtID, customerID,
		); err != nil {
			return err
		}
		// Get output
		if err := tx.QueryRow(
			"SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3",
			warehouseID, districtID, customerID,
		).Scan(&out.cFirst, &out.cMiddle, &out.cLast,
			&out.cStreet1, &out.cStreet2, &out.cCity, &out.cState, &out.cZip,
			&out.cPhone, &out.cSince,
			&out.cCredit, &out.cCreditLimit, &out.cDiscount, &out.cBalance); err != nil {
			return err
		}
		if err := tx.QueryRow(
			"SELECT w_street_1, w_street_2, w_city, w_state, w_zip FROM warehouse WHERE w_id = $1",
			warehouseID,
		).Scan(&out.wStreet1, &out.wStreet2, &out.wCity, &out.wState, &out.wZip); err != nil {
			return err
		}
		if err := tx.QueryRow(
			"SELECT d_street_1, d_street_2, d_city, d_state, d_zip FROM district WHERE d_w_id = $1 AND d_id = $2",
			warehouseID, districtID,
		).Scan(&out.dStreet1, &out.dStreet2, &out.dCity, &out.dState, &out.dZip); err != nil {
			return err
		}
		return nil
	}); err != nil {
		fmt.Fprintln(d.errOut, "run payment txn failed:", err)
		return 0
	}
	duration := time.Since(start)
	// Output
	fmt.Fprintln(d.out, out.cFirst, out.cMiddle, out.cLast)
	fmt.Fprintln(d.out, out.cStreet1, out.cStreet2, out.cCity, out.cState, out.cZip)
	fmt.Fprintln(d.out, out.cPhone, out.cSince, out.cCredit, out.cCreditLimit, out.cDiscount, out.cBalance)
	fmt.Fprintln(d.out, out.wStreet1, out.wStreet2, out.wCity, out.wState, out.wZip)
	fmt.Fprintln(d.out, out.dStreet1, out.dStreet2, out.dCity, out.dState, out.dZip)
	fmt.Fprintln(d.out, payment)
	fmt.Fprintln(d.out, "[Payment done]")
	return duration
}
