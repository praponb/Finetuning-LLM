WITH account_summary AS (
    SELECT account_id, MAX(amount) - MIN(amount) AS amount_range, MIN(txn_time) AS min_txn_time
    FROM transactions
    GROUP BY account_id
)
SELECT a.opened_date, as.amount_range, as.min_txn_time
FROM accounts a
INNER JOIN account_summary as ON a.id = as.account_id
WHERE as.amount_range > 77
ORDER BY a.opened_date
