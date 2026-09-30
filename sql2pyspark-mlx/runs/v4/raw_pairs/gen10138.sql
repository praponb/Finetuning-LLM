SELECT id, start_date, customer_id FROM subscriptions WHERE start_date = (SELECT MAX(start_date) FROM subscriptions) ORDER BY customer_id;
