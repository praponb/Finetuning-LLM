SELECT last_restock
FROM inventory
WHERE product_id IN (1, 3, 8)
UNION
SELECT order_date
FROM orders
