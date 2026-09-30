SELECT supplier_id, MIN(launch_date) AS first_launch_date FROM products WHERE category = 'Electronics' GROUP BY supplier_id ORDER BY supplier_id ASC, first_launch_date ASC LIMIT 10;
