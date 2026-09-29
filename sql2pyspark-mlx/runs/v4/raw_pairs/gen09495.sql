WITH transactions_by_account AS (
    SELECT account_id, MAX(txn_time) AS last_txn_time
    FROM transactions
    GROUP BY account_id
)
SELECT a.customer_id, tba.last_txn_time
FROM accounts a
INNER JOIN transactions_by_account tba ON a.id = tba.account_id;
