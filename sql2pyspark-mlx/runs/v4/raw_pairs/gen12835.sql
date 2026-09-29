SELECT delivered_date FROM shipments WHERE status <> 'LOST' UNION ALL SELECT end_date FROM subscriptions
