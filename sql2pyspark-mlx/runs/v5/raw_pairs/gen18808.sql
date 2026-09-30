SELECT
    id,
    plan,
    monthly_fee,
    AVG(monthly_fee) OVER (PARTITION BY plan) AS plan_avg_monthly_fee
FROM subscriptions
ORDER BY id DESC
