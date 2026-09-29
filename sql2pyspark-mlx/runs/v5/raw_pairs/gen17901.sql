SELECT
    start_date,
    plan,
    status,
    UPPER(plan) AS plan_upper
FROM subscriptions
WHERE start_date BETWEEN '2023-08-18' AND '2023-10-01' AND plan IN ('enterprise', 'premium');
