SELECT last_restock, 'inventory' AS origin
FROM inventory
UNION
SELECT end_date AS last_restock, 'subscription' AS origin
FROM subscriptions
ORDER BY origin
