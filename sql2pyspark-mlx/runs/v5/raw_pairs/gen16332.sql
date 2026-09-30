WITH long_format AS (
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
        val FOR metric_name IN (cost, price)
    )
)
SELECT
    metric_name,
    ROUND(SUM(val) / NULLIF(COUNT(CASE WHEN metric_name = 'price' THEN 1 END), 0), 2) AS val_per_price,
    COUNT_IF(metric_name = 'cost') AS cost_events,
    COUNT_IF(metric_name = 'price') AS price_events,
    SUM(CASE WHEN metric_name = 'price' THEN val ELSE 0 END) AS price_val,
    MAX(val) FILTER (WHERE metric_name = 'cost') AS cost_max_val
FROM long_format
GROUP BY metric_name
ORDER BY cost_events DESC;
