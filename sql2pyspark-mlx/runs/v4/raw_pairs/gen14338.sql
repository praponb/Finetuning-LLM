SELECT start_date
FROM subscriptions
UNION ALL
SELECT order_date start_date
FROM orders
