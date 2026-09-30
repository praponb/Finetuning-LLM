SELECT start_date FROM subscriptions EXCEPT SELECT delivered_date AS start_date FROM shipments WHERE carrier IS NULL;
