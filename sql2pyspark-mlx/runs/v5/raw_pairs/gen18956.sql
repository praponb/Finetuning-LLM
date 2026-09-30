SELECT end_date AS event_date FROM subscriptions WHERE status = 'EXPIRED' UNION SELECT signup_date AS event_date FROM customers WHERE segment = 'Consumer'
