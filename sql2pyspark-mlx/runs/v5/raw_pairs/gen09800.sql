SELECT monthly_fee, end_date, DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
FROM subscriptions
WHERE MONTH(start_date) IN (8, 9, 10)
ORDER BY end_date DESC;
