WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DAYOFWEEK(paid_at) AS paid_at_dayofweek
    FROM payments
    WHERE status = 'REFUNDED'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) AS prev_paid_at
    FROM filtered
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Multi-level aggregation
SELECT
    method,
    session_no,
    order_id,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    ROUND(SUM(session_no) / NULLIF(COUNT(CASE WHEN method = 'promptpay' THEN 1 END), 0), 2) AS session_no_per_promptpay
FROM islands
GROUP BY method, session_no, order_id
ORDER BY session_no_per_promptpay DESC NULLS FIRST;
