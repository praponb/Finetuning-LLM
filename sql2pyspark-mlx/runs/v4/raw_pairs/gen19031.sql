SELECT launch_date FROM products WHERE brand IN ('Acme', 'Initech') UNION SELECT start_date FROM subscriptions ORDER BY launch_date;
