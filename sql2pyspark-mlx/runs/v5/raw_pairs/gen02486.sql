SELECT id, plan, monthly_fee, SUM(monthly_fee) OVER (PARTITION BY plan) AS plan_sum_monthly_fee FROM subscriptions;
