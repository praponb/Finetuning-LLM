SELECT end_date FROM subscriptions WHERE customer_id IS NOT NULL EXCEPT SELECT delivered_date FROM shipments
