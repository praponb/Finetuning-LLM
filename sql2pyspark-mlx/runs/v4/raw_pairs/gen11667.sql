WITH flagged AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM transactions
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    id,
    account_id,
    txn_type,
    amount,
    is_new_burst,
    session_no
FROM sessionized
