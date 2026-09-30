WITH flagged AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessions_payments AS (
    SELECT
        id,
        order_id,
        paid_at,
        status,
        SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    status,
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS success_count,
    APPROX_PERCENTILE(island_id, 0.95) AS island_id_p95
FROM sessions_payments
GROUP BY status, DATE_TRUNC('QUARTER', paid_at)
ORDER BY quarter_start DESC
