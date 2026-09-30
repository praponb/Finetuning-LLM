WITH base AS (
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
        LENGTH(brand) AS brand_length
    FROM products
    WHERE supplier_id IS NOT NULL
      AND cost BETWEEN 270 AND 490
)
SELECT
    id,
    category,
    brand,
    cost,
    tags,
    supplier_id,
    COUNT(brand_length) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_brand_length,
    MAX(brand_length) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_brand_length,
    AVG(brand_length) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_brand_length
FROM base
WHERE category IN ('Fashion', 'Home', 'Sports')
