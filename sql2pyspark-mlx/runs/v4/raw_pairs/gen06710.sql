WITH transactions_by_account AS (
    SELECT account_id, MIN(txn_time) AS first_txn_time, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
    FROM transactions
    GROUP BY account_id
)
SELECT a.customer_id, tba.first_txn_time, tba.txn_type_values
FROM accounts a
LEFT JOIN transactions_by_account tba ON a.id = tba.account_id
