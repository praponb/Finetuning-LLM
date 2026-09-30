SELECT id, customer_id, end_date FROM subscriptions WHERE customer_id IN (2, 6, 8, 15) UNION SELECT id, customer_id, end_date FROM subscriptions WHERE status = 'PAUSED'
