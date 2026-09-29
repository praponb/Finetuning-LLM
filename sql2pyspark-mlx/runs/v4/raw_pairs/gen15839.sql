SELECT opened_date, 'store' AS source FROM stores UNION ALL SELECT end_date, 'subscription' AS source FROM subscriptions WHERE status = 'ACTIVE';
