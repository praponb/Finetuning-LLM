SELECT DISTINCT plan
FROM subscriptions
WHERE plan <> 'standard'
ORDER BY plan ASC
LIMIT 10
