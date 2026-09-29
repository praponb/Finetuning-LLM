SELECT * FROM (SELECT id, order_id, quantity, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS rn FROM order_items WHERE product_id = 36) AS t WHERE rn <= 5
