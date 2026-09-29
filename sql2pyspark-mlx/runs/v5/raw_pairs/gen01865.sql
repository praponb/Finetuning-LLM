SELECT status
FROM subscriptions
EXCEPT
SELECT status
FROM subscriptions
WHERE customer_id IS NULL
