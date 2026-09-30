SELECT opened_date AS event_date
FROM accounts
WHERE balance < 45000
UNION
SELECT order_date AS event_date
FROM orders
