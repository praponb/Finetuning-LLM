WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN method IN ('card', 'promptpay') THEN 'Core' ELSE 'Other' END method_group
    FROM payments
    WHERE paid_at > TO_TIMESTAMP('2023-06-25 15:00:00')
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    status,
    SUM(amount) AS total_amount,
    STDDEV_SAMP(amount) AS amount_stddev,
    SIZE(COLLECT_SET(method)) AS num_methods,
    APPROX_PERCENTILE(amount, 0.5) AS amount_p50
FROM filtered
GROUP BY DATE_TRUNC('QUARTER', paid_at), DATE_TRUNC('YEAR', paid_at), status;
