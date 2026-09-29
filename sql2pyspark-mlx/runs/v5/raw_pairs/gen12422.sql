WITH recent_transactions AS (
    SELECT *, DATE_TRUNC('week', txn_time) AS txn_time_period
    FROM transactions
    WHERE is_flagged
),
agg AS (
    SELECT merchant, AVG(amount) AS avg_amount
    FROM recent_transactions
    GROUP BY merchant
)
SELECT merchant, avg_amount
FROM agg
ORDER BY merchant
LIMIT 1
