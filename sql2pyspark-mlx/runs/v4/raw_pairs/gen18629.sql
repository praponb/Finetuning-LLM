SELECT * FROM orders QUALIFY RANK() OVER (PARTITION BY store_id, status ORDER BY discount DESC) = 1
