SELECT launch_date, product_name FROM products WHERE MONTH(launch_date) IN (1, 5, 7) UNION SELECT launch_date, product_name FROM products WHERE cost >= 240 ORDER BY launch_date;
