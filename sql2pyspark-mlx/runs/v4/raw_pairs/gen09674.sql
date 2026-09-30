WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(amount, 1) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_amount,
        LAG(amount, 1, 0) OVER (PARTITION BY account_id ORDER BY amount, id) AS prev_amount_2
    FROM transactions
)
-- Aggregate with conditional metrics
SELECT
    txn_type,
    merchant,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS span_minutes,
    MAX(txn_time) AS last_seen,
    SUM(CASE WHEN txn_type = 'DEBIT' THEN amount ELSE 0 END) AS debit_amount
FROM metrics
GROUP BY txn_type, merchant
HAVING debit_amount > 0
