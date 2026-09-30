SELECT launch_date FROM products INTERSECT SELECT start_date FROM subscriptions WHERE plan NOT IN ('basic', 'enterprise', 'standard')
