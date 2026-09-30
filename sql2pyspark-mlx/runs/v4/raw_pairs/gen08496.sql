SELECT id, category, launch_date, price, LEAD(price, 2) OVER (PARTITION BY category ORDER BY launch_date, id) AS next_price FROM products WHERE price BETWEEN 600 AND 1000 ORDER BY price DESC;
