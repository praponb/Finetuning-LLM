SELECT stock_qty, launch_date, product_name FROM products WHERE UPPER(product_name) LIKE '%ESK%' AND brand IS NOT NULL ORDER BY product_name;
