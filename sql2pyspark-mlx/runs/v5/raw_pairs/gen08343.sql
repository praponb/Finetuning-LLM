WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        ROUND(units * 1.15, 2) AS adjusted_units
    FROM store_sales
    WHERE revenue < 1600
)
SELECT
    id,
    product_id,
    revenue,
    adjusted_units,
    AVG(revenue) OVER (PARTITION BY store_id, product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_revenue,
    MAX(revenue) OVER (PARTITION BY store_id, product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_revenue,
    MIN(revenue) OVER (PARTITION BY store_id, product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_revenue,
    SUM(units) OVER (ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units
FROM cleaned
WHERE adjusted_units < 70;
