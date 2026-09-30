WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label,
        COALESCE(merchant, 'UNKNOWN') AS resolved_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
      AND merchant IS DISTINCT FROM 'Lazada'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_txn_time
    FROM cleaned
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        resolved_merchant,
        SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
-- Grouped metrics
SELECT
    session_id,
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    ROUND(AVG(session_id), 2) AS avg_session_id,
    RANK() OVER (PARTITION BY session_id ORDER BY ROUND(AVG(session_id), 2) DESC) AS avg_session_id_rank
FROM sessions_transactions
GROUP BY 1, 2
