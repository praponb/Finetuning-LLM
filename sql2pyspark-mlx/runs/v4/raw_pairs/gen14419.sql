WITH base AS (
    SELECT *
    FROM transactions
    WHERE amount BETWEEN 1900 AND 3500
),
summary AS (
    SELECT txn_type, DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month, COUNT(DISTINCT amount) AS unique_txn_types
    FROM base
    GROUP BY txn_type, DATE_FORMAT(txn_time, 'yyyy-MM')
)
SELECT txn_type, txn_month, unique_txn_types
FROM summary
WHERE unique_txn_types > 7
ORDER BY unique_txn_types
