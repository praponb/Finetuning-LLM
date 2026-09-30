SELECT start_date, UPPER(plan) AS plan_upper, UPPER(plan) AS plan_upper_2
FROM subscriptions
WHERE status = 'ACTIVE' OR plan IN ('standard', 'basic')
