SELECT id, plan, DATEDIFF(CURRENT_DATE(), start_date) AS days_since_start_date FROM subscriptions WHERE status NOT IN ('EXPIRED', 'PAUSED', 'CANCELLED');
