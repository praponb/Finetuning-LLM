WITH with_prev AS (
    SELECT *, LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
gaps AS (
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
)
SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
FROM gaps;
