SELECT start_date, end_date
FROM subscriptions
WHERE start_date >= '2023-10-01'
UNION
SELECT start_date, end_date
FROM subscriptions
WHERE MONTH(end_date) IN (6, 7, 8)
