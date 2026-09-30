SELECT
    id,
    plan,
    start_date,
    status
FROM subscriptions
WHERE plan IN ('enterprise', 'standard');
