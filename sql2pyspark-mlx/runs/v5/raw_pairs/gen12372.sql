WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        is_flagged,
        SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    id,
    account_id,
    txn_time,
    island_id
FROM sessionized
ORDER BY account_id ASC NULLS FIRST;
