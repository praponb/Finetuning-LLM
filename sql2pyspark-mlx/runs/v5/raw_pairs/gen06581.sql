SELECT status, plan
FROM subscriptions
WHERE monthly_fee IS NULL
EXCEPT
SELECT status, plan
FROM subscriptions
WHERE MONTH(start_date) IN (6, 7, 12)
