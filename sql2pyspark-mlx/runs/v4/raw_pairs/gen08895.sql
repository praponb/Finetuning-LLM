WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(status)) AS status_title,
        CONCAT(status, ', ', method) AS status_method
    FROM payments
    WHERE paid_at >= TIMESTAMP '2024-04-11 19:00:00'
      AND status = 'SUCCESS'
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) AS prev_paid_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    status,
    COUNT(CASE WHEN method = 'cash' THEN 1 END) AS cash_count,
    ROUND(AVG(session_start_flag), 2) AS avg_session_start_flag,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM sessions_payments
GROUP BY status
