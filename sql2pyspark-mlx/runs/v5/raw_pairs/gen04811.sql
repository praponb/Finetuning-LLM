SELECT 'inventory' AS kind, last_restock FROM inventory WHERE quantity BETWEEN 111 AND 234 UNION SELECT 'subscription' AS kind, end_date FROM subscriptions;
