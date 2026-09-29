WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
-- Multi-level aggregation
SELECT
    order_id,
    CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_session_tier,
    status,
    APPROX_PERCENTILE(amount, 0.95) AS amount_p95,
    MAX(paid_at) AS last_seen,
    PERCENTILE_APPROX(amount, 0.9) AS amount_p90,
    PERCENTILE_APPROX(is_new_session, ARRAY(0.25, 0.5, 0.75)) AS is_new_session_quartiles,
    AVG(island_id) FILTER (WHERE method = 'paypal') AS paypal_avg_island_id
FROM islands
GROUP BY ROLLUP (order_id, CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END, status)
ORDER BY is_new_session_tier DESC NULLS FIRST;
