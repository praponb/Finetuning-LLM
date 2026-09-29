SELECT delivered_date
FROM shipments
WHERE shipped_date > '2024-01-01'
UNION
SELECT end_date
FROM subscriptions;
