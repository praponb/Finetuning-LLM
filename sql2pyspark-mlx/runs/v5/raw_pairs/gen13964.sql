WITH metrics_long AS (
    SELECT *
    FROM (
        SELECT
            id,
            customer_id,
            discount,
            shipping_fee
        FROM orders
    ) src
    UNPIVOT INCLUDE NULLS (
        value FOR kpi IN (discount, shipping_fee)
    )
)
SELECT
    kpi,
    CASE WHEN value >= 87 THEN 'LARGE' ELSE 'SMALL' END AS value_tier,
    ROUND(AVG(value), 2) AS avg_value,
    SIZE(COLLECT_SET(kpi)) AS num_kpis,
    PERCENTILE_APPROX(value, 0.5) AS value_p50,
    RANK() OVER (PARTITION BY kpi ORDER BY ROUND(AVG(value), 2) DESC) AS avg_value_rank
FROM metrics_long
GROUP BY 1, 2
ORDER BY kpi DESC
