SELECT 'order' AS origin, status FROM orders WHERE channel IN ('mobile', 'web') UNION SELECT 'subscription' AS origin, status FROM subscriptions WHERE YEAR(start_date) >= 2024;
