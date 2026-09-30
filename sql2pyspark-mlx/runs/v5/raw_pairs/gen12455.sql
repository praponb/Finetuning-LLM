WITH transactions_by_account AS (
    SELECT account_id, MAX(txn_time) AS last_txn_time
    FROM transactions
    WHERE txn_time >= '2023-07-10'
    GROUP BY account_id
)
SELECT
    a.id,
    a.status,
    a.opened_date,
    tba.last_txn_time
FROM accounts a
LEFT JOIN transactions_by_account tba ON a.id = tba.account_id
ORDER BY status DESC, tba.last_txn_time DESC;
