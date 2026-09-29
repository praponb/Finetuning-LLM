SELECT plan FROM subscriptions WHERE status IN ('EXPIRED', 'PAUSED') UNION ALL SELECT plan FROM subscriptions WHERE customer_id IS NULL
