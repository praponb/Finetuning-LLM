WITH metrics_long AS (
    -- Native SQL UNPIVOT (wide to long)
    SELECT *
    FROM (
        SELECT
            id,
            channel,
            amount,
            shipping_fee
        FROM orders
    ) src
    UNPIVOT (
        metric_value FOR metric_name IN (amount AS AMOUNT_VAL, shipping_fee AS SHIPPING_FEE)
    )
)
-- Multi-level aggregation
SELECT
    metric_name,
    MAX(metric_value) AS max_metric_value,
    SUM(CASE WHEN metric_name = 'AMOUNT_VAL' THEN metric_value ELSE 0 END) AS amount_val_metric_value,
    COUNT(*) AS row_count,
    ROUND(SUM(metric_value) / NULLIF(COUNT(CASE WHEN channel = 'web' THEN 1 END), 0), 2) AS metric_value_per_web,
    PERCENTILE_APPROX(metric_value, ARRAY(0.25, 0.5, 0.75)) AS metric_value_quartiles,
    GROUPING_ID() AS grouping_id
FROM metrics_long
GROUP BY CUBE (metric_name)
HAVING COUNT(*) >= 1
ORDER BY amount_val_metric_value ASC
