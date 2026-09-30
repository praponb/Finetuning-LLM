SELECT QUARTER(end_date) AS end_quarter, SUM(CASE WHEN plan = 'enterprise' THEN 1 ELSE 0 END) AS enterprise_count FROM subscriptions WHERE customer_id IN (15, 27, 33, 40) GROUP BY 1;
