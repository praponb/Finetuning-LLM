WITH base AS (
    SELECT *
    FROM transactions
    WHERE amount <= 2500
),
transactions_stats AS (
    SELECT DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month, ROUND(STDDEV(amount), 2) AS stddev_amount, SUM(amount) AS total_amount
    FROM base
    GROUP BY DATE_FORMAT(txn_time, 'yyyy-MM')
)
SELECT txn_month, stddev_amount, total_amount
FROM transactions_stats;
