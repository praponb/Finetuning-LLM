SELECT last_restock
FROM inventory
WHERE product_id IN (3, 4, 9, 10)
EXCEPT
SELECT start_date
FROM subscriptions
