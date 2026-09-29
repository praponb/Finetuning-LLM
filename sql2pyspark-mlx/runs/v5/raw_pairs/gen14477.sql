SELECT opened_date AS event_date
FROM accounts
EXCEPT
SELECT start_date AS event_date
FROM subscriptions
WHERE plan IN ('premium', 'basic')
ORDER BY event_date DESC
