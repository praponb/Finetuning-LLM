SELECT delivered_date, 'shipment' AS record_type FROM shipments UNION SELECT start_date, 'subscription' AS record_type FROM subscriptions WHERE customer_id IS NULL ORDER BY delivered_date
