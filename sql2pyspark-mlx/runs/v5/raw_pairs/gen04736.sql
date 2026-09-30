SELECT id, start_date, status
FROM subscriptions
WHERE monthly_fee BETWEEN 160 AND 200
UNION
SELECT id, start_date, status
FROM subscriptions
WHERE plan IN ('premium', 'enterprise', 'standard')
ORDER BY status
