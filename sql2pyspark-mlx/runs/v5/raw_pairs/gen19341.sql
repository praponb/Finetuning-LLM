SELECT start_date, 'subscription' AS record_type FROM subscriptions UNION SELECT order_date AS start_date, 'order' AS record_type FROM orders;
