SELECT id, end_date, monthly_fee
FROM subscriptions
WHERE end_date = (SELECT MAX(end_date) FROM subscriptions)
ORDER BY monthly_fee
