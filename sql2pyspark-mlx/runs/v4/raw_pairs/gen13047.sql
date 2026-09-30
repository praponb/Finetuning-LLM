WITH filtered AS (
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        GREATEST(stock_qty, cost) AS greatest_stock_qty_cost,
        SUBSTRING(category, 1, 4) AS category_prefix
    FROM products
    WHERE launch_date > DATE '2023-08-27'
),
base AS (
    SELECT
        id,
        product_name,
        brand,
        cost,
        launch_date,
        tags,
        supplier_id,
        greatest_stock_qty_cost,
        category_prefix,
        CONCAT(product_name, ' - ', brand) AS product_name_brand
    FROM filtered
    WHERE price > 430
),
kept_keys AS (
    SELECT id
    FROM base
    EXCEPT DISTINCT
    SELECT id
    FROM products
    WHERE price >= 460
),
verified AS (
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    product_name,
    brand,
    cost,
    launch_date,
    supplier_id,
    greatest_stock_qty_cost,
    category_prefix,
    product_name_brand
FROM verified
