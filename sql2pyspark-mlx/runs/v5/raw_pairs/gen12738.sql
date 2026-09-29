SELECT *
FROM subscriptions
WHERE status IN ('ACTIVE', 'CANCELLED', 'PAUSED') AND plan NOT IN ('premium', 'enterprise', 'basic')
ORDER BY monthly_fee, id
LIMIT 10;
