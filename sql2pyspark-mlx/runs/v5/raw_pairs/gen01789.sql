SELECT discount, status
FROM orders
WHERE customer_id IN (26, 30, 35) AND store_id IN (4, 15, 18, 23)
ORDER BY discount DESC
