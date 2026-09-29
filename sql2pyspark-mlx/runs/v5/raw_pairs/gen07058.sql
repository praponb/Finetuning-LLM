SELECT last_restock
FROM inventory
WHERE product_id IN (3, 6)
UNION ALL
SELECT start_date
FROM subscriptions
