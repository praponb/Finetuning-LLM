SELECT *
FROM orders
WHERE amount <= 540 AND status IN ('SHIPPED', 'PENDING', 'CANCELLED') AND store_id IS NULL
