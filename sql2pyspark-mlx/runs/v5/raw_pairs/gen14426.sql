SELECT id, plan, customer_id, start_date FROM subscriptions WHERE status IN ('CANCELLED', 'ACTIVE')
