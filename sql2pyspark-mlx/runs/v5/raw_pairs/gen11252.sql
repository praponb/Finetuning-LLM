SELECT
    id,
    plan,
    monthly_fee,
    customer_id,
    UPPER(status) AS status_upper,
    SUBSTRING(plan, 1, 4) AS plan_prefix
FROM subscriptions
WHERE customer_id IN (3, 4, 18);
