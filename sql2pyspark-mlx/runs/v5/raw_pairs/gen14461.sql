SELECT id, category, launch_date, price, ROUND(AVG(price) OVER (PARTITION BY category ORDER BY launch_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS price_moving_avg_4 FROM products;
