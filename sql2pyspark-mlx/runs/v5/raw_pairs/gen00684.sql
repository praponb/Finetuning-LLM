SELECT
    id,
    plan,
    monthly_fee,
    MAX(monthly_fee) OVER (PARTITION BY plan) AS plan_max_monthly_fee,
    MAX(monthly_fee) OVER (PARTITION BY plan) AS plan_max_monthly_fee_2
FROM subscriptions
ORDER BY 1
