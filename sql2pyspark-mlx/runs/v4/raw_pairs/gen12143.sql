SELECT
    id,
    brand,
    launch_date,
    stock_qty,
    SUBSTRING(category, 1, 2) AS category_prefix
FROM products
