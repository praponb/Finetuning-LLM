WITH account_totals AS (
    SELECT account_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
    FROM transactions
    GROUP BY account_id
)
SELECT a.customer_id, a.opened_date, at.txn_type_values
FROM accounts a
INNER JOIN account_totals at ON a.id = at.account_id
ORDER BY opened_date, a.customer_id DESC
