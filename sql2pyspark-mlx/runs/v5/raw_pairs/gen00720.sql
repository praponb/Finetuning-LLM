WITH prev_events AS (
    SELECT *, LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
with_gaps AS (
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    DATE_TRUNC('MONTH', prev_txn_time) AS activity_month,
    SUM(CASE WHEN merchant = 'Shell' THEN amount ELSE 0 END) AS shell_amount,
    COUNT(*) AS cnt
FROM sessionized
GROUP BY ROLLUP (DATE_TRUNC('MONTH', prev_txn_time))
HAVING cnt >= 1
ORDER BY activity_month DESC;
