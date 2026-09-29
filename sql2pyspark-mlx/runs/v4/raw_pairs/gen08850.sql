WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM sessions_transactions
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessions_transactions_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
joined AS (
    -- Attach accounts attributes
    SELECT e.*, p.opened_date
    FROM sessions_transactions_2 e
    INNER JOIN accounts p
        ON e.account_id = p.id
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_txn_time_2
    FROM joined
),
flagged_2 AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time_2 IS NULL OR txn_time > prev_txn_time_2 + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged_2
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN island_id >= 6 THEN 'HIGH' ELSE 'LOW' END AS island_id_tier,
    COUNT(DISTINCT account_id) AS distinct_account_ids,
    SUM(amount) FILTER (WHERE merchant = 'Starbucks') AS starbucks_total_amount,
    COUNT(*) FILTER (WHERE merchant = 'Shell') AS shell_rows
FROM sessionized
GROUP BY CUBE (CASE WHEN island_id >= 6 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY island_id_tier;
