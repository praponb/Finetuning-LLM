SELECT status, end_date, CASE WHEN plan = 'premium' THEN 1 ELSE 0 END AS is_premium FROM subscriptions
