WITH flagged AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    CASE WHEN burst_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS burst_id_tier,
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success_rows,
    STDDEV_SAMP(amount) AS amount_stddev,
    APPROX_PERCENTILE(amount, 0.5) AS amount_p50,
    ROUND(AVG(session_start_flag), 2) AS avg_session_start_flag,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', paid_at) ORDER BY ROUND(AVG(session_start_flag), 2) DESC) AS avg_session_start_flag_percent_rank
FROM sessionized
GROUP BY 1, 2, 3;
