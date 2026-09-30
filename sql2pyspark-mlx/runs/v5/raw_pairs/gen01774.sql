SELECT order_date
FROM orders
WHERE amount < 540
EXCEPT
SELECT end_date
FROM subscriptions
