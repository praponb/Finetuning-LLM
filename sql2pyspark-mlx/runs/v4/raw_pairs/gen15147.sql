WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(method) AS method_length,
        CASE WHEN method IN ('paypal', 'card') THEN 'Priority' ELSE 'Other' END AS method_group
    FROM payments
    WHERE order_id IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_session_tier,
    method,
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count,
    COUNT(*) AS cnt,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    PERCENTILE_APPROX(amount, 0.95) AS amount_p95
FROM sessionized
GROUP BY ROLLUP (CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END, method, DATE_TRUNC('YEAR', paid_at))
