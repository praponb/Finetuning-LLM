SELECT
    id,
    category,
    launch_date,
    cost,
    stock_qty,
    LAG(cost, 1, 0) OVER (PARTITION BY category ORDER BY launch_date, id) AS prev_cost,
    LEAD(stock_qty, 1) OVER (ORDER BY launch_date, id) AS next_stock_qty
FROM products
WHERE category != 'Electronics';
