SELECT COUNT(*) AS total_count, SUM(CASE WHEN plan = 'basic' THEN 1 ELSE 0 END) AS basic_count FROM subscriptions
