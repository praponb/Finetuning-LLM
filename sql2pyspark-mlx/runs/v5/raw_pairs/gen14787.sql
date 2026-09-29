WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(brand) AS brand_upper
    FROM products
    WHERE brand IS DISTINCT FROM 'Initech'
      AND launch_date > DATE '2023-09-17'
)
-- Window metrics over several different partitions
SELECT
    id,
    product_name,
    brand,
    cost,
    stock_qty,
    launch_date,
    supplier_id,
    brand_upper,
    ROUND(cost / SUM(cost) OVER (PARTITION BY brand, supplier_id), 4) AS cost_share,
    FIRST_VALUE(price) IGNORE NULLS OVER (PARTITION BY supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_price,
    LAG(stock_qty, 1) OVER (PARTITION BY supplier_id ORDER BY cost, id) AS prev_stock_qty
FROM base
