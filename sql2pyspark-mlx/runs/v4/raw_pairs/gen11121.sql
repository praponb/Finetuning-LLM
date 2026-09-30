SELECT
    id,
    monthly_fee,
    customer_id,
    plan,
    LOWER(plan) AS plan_lower
FROM subscriptions
WHERE (customer_id = 30 AND MONTH(end_date) IN (1, 2, 8)) OR start_date <= '2024-09-01';
