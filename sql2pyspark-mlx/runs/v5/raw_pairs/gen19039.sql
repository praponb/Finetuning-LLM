SELECT id, start_date, plan, SUBSTRING(plan, 1, 4) AS plan_prefix FROM subscriptions WHERE customer_id IS NULL AND customer_id IS NOT NULL ORDER BY plan_prefix, id DESC;
