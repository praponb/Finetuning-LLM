SELECT status, MAX(start_date) AS last_start_date, COUNT(monthly_fee) AS monthly_fee_count
FROM subscriptions
WHERE monthly_fee < 90 AND monthly_fee <= 210
GROUP BY status
HAVING COUNT(monthly_fee) >= 3
ORDER BY last_start_date, status DESC;
