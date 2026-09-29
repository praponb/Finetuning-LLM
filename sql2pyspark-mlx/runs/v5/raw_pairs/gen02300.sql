SELECT
    id,
    brand,
    launch_date,
    cost,
    LAG(cost, 1) OVER (PARTITION BY brand ORDER BY launch_date, id) AS prev_cost
FROM products
WHERE stock_qty BETWEEN 256 AND 351
ORDER BY prev_cost ASC, launch_date ASC
