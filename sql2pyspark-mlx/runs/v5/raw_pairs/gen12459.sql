WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        stock_qty - cost AS stock_qty_minus_cost,
        COALESCE(brand, 'UNKNOWN') AS brand_clean
    FROM products
    WHERE brand IS NOT NULL
)
-- Join a dimension and aggregate
SELECT
    dim.country,
    dim.city,
    COUNT(*) AS cnt,
    MAX(src.stock_qty_minus_cost) AS max_stock_qty_minus_cost,
    MAX(src.price) AS max_price,
    GROUPING_ID() AS level_id
FROM filtered src
INNER JOIN suppliers dim ON src.supplier_id = dim.id
GROUP BY GROUPING SETS ((dim.country, dim.city), (dim.country), ())
ORDER BY city
