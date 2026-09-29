WITH src AS (
    SELECT *
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 10 AND 13
),
Summary AS (
    SELECT merchant, COUNT(DISTINCT amount) AS unique_txn_types
    FROM src
    GROUP BY merchant
)
SELECT *
FROM Summary
ORDER BY unique_txn_types DESC, merchant
LIMIT 10;
