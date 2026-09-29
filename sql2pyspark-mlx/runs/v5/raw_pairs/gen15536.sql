SELECT
    id,
    supplier_id,
    launch_date,
    price,
    brand,
    stock_qty,
    SUM(price) OVER (PARTITION BY supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_price,
    stock_qty - LAG(stock_qty, 2) OVER (PARTITION BY brand ORDER BY launch_date, id) AS stock_qty_change
FROM products
ORDER BY stock_qty_change DESC
