SELECT customer_id
FROM subscriptions
EXCEPT DISTINCT
SELECT customer_id
FROM subscriptions
WHERE start_date > '2023-08-01'
