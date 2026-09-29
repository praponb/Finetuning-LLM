WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(paid_at AS DATE) paid_at_day
    FROM payments
    WHERE method IS NOT NULL
      AND method NOT IN ('bank_transfer', 'card', 'paypal')
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount >= 1720 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(CASE WHEN method = 'promptpay' THEN 1 END) AS promptpay_count,
    COUNT(DISTINCT status) AS distinct_status,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    PERCENTILE_APPROX(is_new_session, ARRAY(0.25, 0.5, 0.75)) AS is_new_session_quartiles,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM islands
GROUP BY CASE WHEN amount >= 1720 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', paid_at)
ORDER BY distinct_status DESC NULLS FIRST
