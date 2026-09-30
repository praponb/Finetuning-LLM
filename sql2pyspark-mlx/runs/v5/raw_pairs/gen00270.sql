SELECT
    id,
    launch_date,
    cost,
    cost - LAG(cost, 1) OVER (ORDER BY launch_date, id) AS cost_change
FROM products
ORDER BY launch_date, cost DESC;
