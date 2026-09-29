SELECT id, brand, launch_date, cost, LAG(cost, 1) OVER (PARTITION BY brand ORDER BY launch_date, id) AS prev_cost FROM products WHERE MONTH(launch_date) IN (5, 8, 9);
