SELECT start_date FROM subscriptions UNION ALL SELECT delivered_date AS start_date FROM shipments WHERE order_id = 6;
