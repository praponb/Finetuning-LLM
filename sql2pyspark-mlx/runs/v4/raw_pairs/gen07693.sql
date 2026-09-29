SELECT start_date AS event_date
FROM subscriptions
UNION
SELECT order_date AS event_date
FROM orders;
