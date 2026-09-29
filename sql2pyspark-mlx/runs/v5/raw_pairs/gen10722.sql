SELECT status, monthly_fee, CAST(monthly_fee AS INT) AS monthly_fee_int
FROM subscriptions
WHERE (plan = 'enterprise' AND monthly_fee < 180) OR end_date >= '2024-07-04'
