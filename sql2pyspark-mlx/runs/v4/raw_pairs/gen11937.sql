SELECT end_date
FROM subscriptions
UNION ALL
SELECT last_restock
FROM inventory
ORDER BY end_date DESC;
