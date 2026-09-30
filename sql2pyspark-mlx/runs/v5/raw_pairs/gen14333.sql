SELECT
    id,
    category,
    launch_date,
    stock_qty,
    SUM(stock_qty) OVER (PARTITION BY category ORDER BY launch_date, id) AS running_stock_qty
FROM products
WHERE product_name NOT LIKE '%Runn%'
