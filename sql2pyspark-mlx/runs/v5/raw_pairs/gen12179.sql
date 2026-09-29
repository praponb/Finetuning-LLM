WITH account_totals AS (
    SELECT account_id, SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE 0 END) AS credit_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
    FROM transactions
    GROUP BY account_id
)
SELECT a.customer_id, at.credit_amount, at.txn_type_values
FROM accounts a
INNER JOIN account_totals at ON a.id = at.account_id
WHERE at.credit_amount >= 55
