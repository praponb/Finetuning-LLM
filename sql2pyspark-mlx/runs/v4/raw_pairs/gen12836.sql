SELECT id, start_date, plan
FROM subscriptions
WHERE start_date >= (SELECT MAX(start_date) FROM subscriptions WHERE monthly_fee BETWEEN 140 AND 200)
ORDER BY start_date DESC
