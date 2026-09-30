SELECT monthly_fee, plan, LENGTH(status) AS status_length
FROM subscriptions
WHERE plan <> 'enterprise' AND monthly_fee >= 110
