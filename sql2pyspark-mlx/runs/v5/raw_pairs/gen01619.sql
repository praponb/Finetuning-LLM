SELECT status, plan, ROUND((100 * SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_active FROM subscriptions WHERE MONTH(start_date) IN (2, 10, 12) GROUP BY status, plan
