WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    STDDEV_SAMP(amount) AS amount_stddev,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_rows,
    MIN(paid_at) AS started_at,
    ROUND(SUM(session_start_flag) / NULLIF(COUNT(CASE WHEN status = 'FAILED' THEN 1 END), 0), 2) AS session_start_flag_per_failed,
    STDDEV_SAMP(amount) AS amount_stddev_2
FROM islands
GROUP BY DATE_TRUNC('MONTH', paid_at)
ORDER BY amount_stddev_2 NULLS LAST;
