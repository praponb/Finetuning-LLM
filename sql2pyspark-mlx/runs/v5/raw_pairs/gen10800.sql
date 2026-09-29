SELECT 'customer' AS record_type, signup_date FROM customers WHERE country != 'JP' UNION SELECT 'subscription' AS record_type, end_date FROM subscriptions WHERE monthly_fee BETWEEN 130 AND 220;
