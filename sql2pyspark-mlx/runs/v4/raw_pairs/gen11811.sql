SELECT signup_date, 'customer' AS source FROM customers UNION SELECT end_date AS signup_date, 'subscription' AS source FROM subscriptions WHERE customer_id = 40;
