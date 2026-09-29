WITH filtered_transactions AS (
    SELECT *
    FROM transactions
    WHERE amount <= 2500
),
txn_year_summary AS (
    SELECT YEAR(txn_time) AS txn_year, COUNT(CASE WHEN txn_type = 'CREDIT' THEN 1 END) AS credit_count, MAX(txn_time) AS latest_txn_time
    FROM filtered_transactions
    GROUP BY YEAR(txn_time)
)
SELECT txn_year, latest_txn_time
FROM txn_year_summary
WHERE credit_count >= 4
ORDER BY latest_txn_time, 1 DESC
