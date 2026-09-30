SELECT id, customer_id, status, channel, amount, discount FROM orders QUALIFY RANK() OVER (PARTITION BY customer_id, channel ORDER BY discount DESC) = 1
