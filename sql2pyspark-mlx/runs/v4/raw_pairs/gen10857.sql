WITH src AS (
    SELECT *
    FROM transactions
    WHERE account_id = 8 AND (txn_type IN ('REFUND', 'FEE', 'CREDIT') OR account_id IN (8, 34))
),
agg AS (
    SELECT txn_type, MAX(txn_time) AS last_txn_time, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values
    FROM src
    GROUP BY txn_type
)
SELECT *
FROM agg
ORDER BY txn_type, last_txn_time
LIMIT 50;
