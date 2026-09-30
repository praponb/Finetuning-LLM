WITH account_summary AS (
    SELECT account_id, COUNT(amount) AS amount_count, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
    FROM transactions
    WHERE TO_DATE(txn_time) < '2023-03-14'
    GROUP BY account_id
)
SELECT
    a.id,
    a.customer_id,
    as.amount_count,
    as.txn_type_list
FROM accounts a
LEFT JOIN account_summary as ON a.id = as.account_id
WHERE as.amount_count > 4
