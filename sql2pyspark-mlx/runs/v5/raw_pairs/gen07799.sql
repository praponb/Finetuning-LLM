SELECT id, category, launch_date, cost, LEAD(cost, 1) OVER (PARTITION BY category ORDER BY launch_date, id) AS next_cost FROM products
