WITH transactions_by_account AS (
    SELECT account_id, MAX(txn_time) AS latest_txn_time
    FROM transactions
    GROUP BY account_id
)
SELECT a.account_type, a.opened_date, tba.latest_txn_time
FROM accounts a
LEFT JOIN transactions_by_account tba ON a.id = tba.account_id
ORDER BY a.opened_date;
