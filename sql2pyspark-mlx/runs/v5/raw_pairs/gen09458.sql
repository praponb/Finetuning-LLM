SELECT id, product_id, quantity, ROUND(AVG(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS quantity_moving_avg_7 FROM order_items;
