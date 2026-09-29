SELECT id, end_date, ROW_NUMBER() OVER (ORDER BY end_date, id) end_date_row
FROM subscriptions
WHERE customer_id = 23
ORDER BY end_date_row DESC;
