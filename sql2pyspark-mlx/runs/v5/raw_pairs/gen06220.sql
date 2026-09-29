SELECT
    id,
    brand,
    cost,
    stock_qty,
    launch_date,
    supplier_id,
    MAX(price) OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_price,
    SUM(price) OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_price,
    COUNT(price) OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_price
FROM products;
