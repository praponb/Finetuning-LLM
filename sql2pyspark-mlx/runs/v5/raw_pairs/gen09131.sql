SELECT unit_price FROM order_items UNION ALL SELECT cost AS unit_price FROM products WHERE MONTH(launch_date) IN (1, 3, 9) ORDER BY unit_price;
