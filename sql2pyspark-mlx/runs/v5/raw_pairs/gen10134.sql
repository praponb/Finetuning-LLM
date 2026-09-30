SELECT 'order' AS record_type, status FROM orders WHERE order_date >= '2025-04-03' UNION ALL SELECT 'subscription' AS record_type, status FROM subscriptions WHERE customer_id IS NOT NULL
