WITH transactions_by_account AS (
    SELECT account_id, MIN(txn_time) AS min_txn_time, SUM(CASE WHEN txn_type = 'REFUND' THEN amount ELSE 0 END) AS refund_amount
    FROM transactions
    WHERE amount >= 3600
    GROUP BY account_id
)
SELECT
    a.account_type,
    a.status,
    tba.min_txn_time,
    tba.refund_amount
FROM accounts a
LEFT JOIN transactions_by_account tba ON a.id = tba.account_id
WHERE tba.refund_amount > 76;
