SELECT id, end_date, plan FROM subscriptions WHERE plan IN ('standard', 'enterprise', 'basic') UNION SELECT id, end_date, plan FROM subscriptions WHERE status IN ('CANCELLED', 'PAUSED', 'EXPIRED')
