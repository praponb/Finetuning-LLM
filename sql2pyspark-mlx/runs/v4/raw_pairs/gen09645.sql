SELECT status, QUARTER(end_date) AS end_quarter, COUNT(*) AS cnt
FROM subscriptions
GROUP BY 1, 2
ORDER BY status, end_quarter DESC
