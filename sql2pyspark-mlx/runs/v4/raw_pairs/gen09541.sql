SELECT MAX(end_date) AS last_end_date, AVG(monthly_fee) AS avg_monthly_fee
FROM subscriptions
WHERE start_date <= '2025-03-22'
ORDER BY last_end_date;
