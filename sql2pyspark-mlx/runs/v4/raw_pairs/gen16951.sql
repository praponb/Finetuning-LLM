WITH account_totals AS (
    SELECT account_id, MAX(amount) - MIN(amount) AS amount_range, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
    FROM transactions
    GROUP BY account_id
)
SELECT a.account_type, at.amount_range, at.txn_type_values
FROM accounts a
JOIN account_totals at ON a.id = at.account_id
WHERE at.amount_range >= 62
ORDER BY txn_type_values, a.account_type
