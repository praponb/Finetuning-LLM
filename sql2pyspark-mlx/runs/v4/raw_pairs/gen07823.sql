SELECT id, start_date, customer_id, end_date FROM subscriptions WHERE start_date <= (SELECT MIN(start_date) FROM subscriptions) ORDER BY end_date
