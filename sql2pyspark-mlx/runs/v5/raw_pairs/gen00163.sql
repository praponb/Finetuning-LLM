SELECT
    id,
    category,
    launch_date,
    cost,
    SUM(cost) OVER (PARTITION BY category ORDER BY launch_date, id) AS running_cost
FROM products
WHERE supplier_id = 27
