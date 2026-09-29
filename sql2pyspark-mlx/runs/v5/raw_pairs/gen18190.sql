SELECT *, CASE WHEN plan = 'standard' THEN 1 ELSE 0 END AS is_standard FROM subscriptions WHERE customer_id = 2 ORDER BY 8
