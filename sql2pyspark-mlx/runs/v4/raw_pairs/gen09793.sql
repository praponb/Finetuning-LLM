SELECT id, start_date, plan
FROM subscriptions
WHERE plan <> 'standard'
ORDER BY id DESC
LIMIT 1
