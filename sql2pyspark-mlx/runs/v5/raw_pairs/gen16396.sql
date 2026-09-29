WITH base AS (
    SELECT
        *,
        SUBSTRING(status, 1, 3) AS status_prefix,
        COALESCE(method, 'DIRECT') AS method_clean
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-10-25 12:00:00'
      AND status = 'FAILED'
)
SELECT
    method,
    CASE WHEN amount >= 1210 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    APPROX_PERCENTILE(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    ROUND(AVG(amount), 2) AS avg_amount,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) AS event_count,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes,
    STDDEV_SAMP(amount) AS amount_stddev
FROM base
GROUP BY method, amount_tier
ORDER BY avg_amount DESC;
