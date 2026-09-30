SELECT id, monthly_fee, start_date
FROM subscriptions
WHERE start_date >= '2023-09-01' AND plan IN ('standard', 'basic', 'premium')
ORDER BY id, monthly_fee DESC;
