SELECT *
FROM subscriptions
WHERE monthly_fee <= 160 AND plan IN ('premium', 'basic')
ORDER BY id DESC;
