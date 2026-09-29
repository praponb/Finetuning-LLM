SELECT
    id,
    category,
    launch_date,
    stock_qty,
    brand,
    cost,
    SUM(stock_qty) OVER (PARTITION BY category ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty,
    MIN(cost) OVER (PARTITION BY brand) AS brand_min_cost
FROM products
WHERE category IN ('Books', 'Fashion', 'Sports');
