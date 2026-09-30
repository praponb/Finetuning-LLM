WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        ARRAY_CONTAINS(tags, 'sale') AS is_sale,
        COALESCE(brand, 'DIRECT') AS brand_filled
    FROM products
    WHERE launch_date > TO_DATE('2023-10-23')
      AND supplier_id IS NOT NULL
      AND supplier_id IS NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN stock_qty >= 396 THEN 'LARGE' ELSE 'SMALL' END AS stock_qty_tier,
    brand,
    APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    COUNT(DISTINCT supplier_id) AS distinct_supplier_ids,
    AVG(cost) FILTER (WHERE category = 'Sports') AS sports_avg_cost
FROM base
GROUP BY CASE WHEN stock_qty >= 396 THEN 'LARGE' ELSE 'SMALL' END, brand
