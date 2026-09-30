SELECT start_date, monthly_fee, CASE WHEN plan = 'premium' THEN 1 ELSE 0 END is_premium FROM subscriptions ORDER BY monthly_fee ASC, start_date ASC;
