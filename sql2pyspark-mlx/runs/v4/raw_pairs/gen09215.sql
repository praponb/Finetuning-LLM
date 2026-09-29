SELECT status, SUM(CASE WHEN plan = 'enterprise' THEN 1 ELSE 0 END) AS enterprise_count FROM subscriptions GROUP BY status;
