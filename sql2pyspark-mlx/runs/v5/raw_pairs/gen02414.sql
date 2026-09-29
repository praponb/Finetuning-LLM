SELECT last_restock
FROM inventory
INTERSECT
SELECT end_date AS last_restock
FROM subscriptions
ORDER BY last_restock;
