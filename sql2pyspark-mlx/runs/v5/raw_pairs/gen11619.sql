SELECT last_restock AS event_date
FROM inventory
INTERSECT
SELECT end_date AS event_date
FROM subscriptions;
