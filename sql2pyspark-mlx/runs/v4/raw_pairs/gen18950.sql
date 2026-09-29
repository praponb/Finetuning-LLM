SELECT launch_date, 'product' AS source FROM products WHERE ARRAY_CONTAINS(tags, 'premium') UNION SELECT end_date, 'subscription' AS source FROM subscriptions ORDER BY source
