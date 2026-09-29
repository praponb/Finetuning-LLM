SELECT
    id,
    supplier_id,
    category,
    launch_date,
    stock_qty,
    ROUND(AVG(stock_qty) OVER (PARTITION BY supplier_id, category ORDER BY launch_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS stock_qty_moving_avg_4
FROM products;
