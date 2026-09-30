SELECT customer_id, start_date FROM subscriptions WHERE YEAR(start_date) = 2023 UNION SELECT customer_id, start_date FROM subscriptions WHERE status = 'CANCELLED' ORDER BY start_date DESC;
