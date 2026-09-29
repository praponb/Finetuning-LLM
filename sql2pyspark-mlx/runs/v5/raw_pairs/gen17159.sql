SELECT id, customer_id, end_date, start_date, TRIM(plan) AS clean_plan FROM subscriptions WHERE plan IN ('enterprise', 'basic')
