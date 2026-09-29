SELECT launch_date
FROM products
UNION ALL
SELECT end_date
FROM subscriptions
WHERE customer_id IN (10, 35)
