WITH with_prev AS (
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
flagged AS (
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    s.id,
    s.account_id,
    s.amount,
    s.is_flagged,
    s.prev_txn_time,
    s.session_start_flag,
    s.session_no,
    (SELECT AVG(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS avg_transaction_amount
FROM sessionized s
ORDER BY amount DESC
