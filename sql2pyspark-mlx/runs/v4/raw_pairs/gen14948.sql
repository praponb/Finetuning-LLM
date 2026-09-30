WITH unpivoted AS (
    SELECT *
    FROM (
        SELECT
            id,
            supplier_id,
            cost,
            price
        FROM products
    ) src
    UNPIVOT (
        metric_value FOR kpi IN (cost, price)
    )
)
SELECT
    CASE WHEN metric_value >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS metric_value_tier,
    APPROX_PERCENTILE(metric_value, 0.95) AS metric_value_p95,
    SUM(metric_value) AS total_metric_value,
    PERCENTILE_APPROX(metric_value, ARRAY(0.25, 0.5, 0.75)) AS metric_value_quartiles,
    SUM(metric_value) AS total_metric_value_2,
    SUM(metric_value) AS total_metric_value_3
FROM unpivoted
GROUP BY CASE WHEN metric_value >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY total_metric_value_2 DESC
