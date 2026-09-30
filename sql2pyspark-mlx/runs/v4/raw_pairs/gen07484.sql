WITH src AS (
    SELECT *, ROUND(amount * 1.15, 2) adjusted_amount
    FROM transactions
    WHERE account_id = 24 AND merchant IN ('Lazada', 'Amazon', 'Starbucks')
),
summary AS (
    SELECT CASE WHEN txn_type IN ('DEBIT', 'REFUND') THEN 'core' ELSE 'other' END txn_type_group, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) txn_type_values
    FROM src
    GROUP BY CASE WHEN txn_type IN ('DEBIT', 'REFUND') THEN 'core' ELSE 'other' END
)
SELECT txn_type_group, txn_type_values
FROM summary
ORDER BY txn_type_values, txn_type_group
LIMIT 20
