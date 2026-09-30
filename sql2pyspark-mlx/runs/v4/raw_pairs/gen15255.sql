SELECT
    launch_date,
    tags,
    SUBSTRING(category, 1, 2) AS category_prefix,
    ROUND(stock_qty / price, 2) AS stock_qty_per_price
FROM products
WHERE supplier_id IS NOT NULL;
