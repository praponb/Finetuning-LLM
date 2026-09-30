SELECT id, end_date, start_date, customer_id, LOWER(status) AS status_lower FROM subscriptions WHERE MONTH(start_date) IN (9, 10, 11);
