SELECT
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(amount, 0.95) AS amount_p95,
    STDDEV_SAMP(amount) AS amount_stddev,
    ROUND(AVG(amount), 2) AS avg_amount,
    MIN(paid_at) AS first_paid_at,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes
FROM payments
GROUP BY DATE_TRUNC('QUARTER', paid_at), DATE_TRUNC('MONTH', paid_at), DATE_TRUNC('WEEK', paid_at);
