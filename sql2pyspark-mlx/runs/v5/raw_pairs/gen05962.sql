SELECT 'subscription' AS origin, start_date
FROM subscriptions
UNION
SELECT 'inventory' AS origin, last_restock
FROM inventory
WHERE last_restock <= '2025-04-29';
