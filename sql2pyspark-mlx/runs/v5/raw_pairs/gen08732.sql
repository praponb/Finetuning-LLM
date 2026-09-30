SELECT supplier_id, MAX(launch_date) AS last_launch_date FROM products GROUP BY supplier_id;
