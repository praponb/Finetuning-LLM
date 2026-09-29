SELECT CASE WHEN c.age >= 71 THEN 'high' WHEN c.age >= 41 THEN 'mid' ELSE 'low' END AS age_bucket, MAX(s.end_date) AS last_end_date, ROUND((100 * SUM(CASE WHEN s.plan = 'premium' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_premium
FROM subscriptions s
LEFT OUTER JOIN customers c ON c.id = s.customer_id
GROUP BY CASE WHEN c.age >= 71 THEN 'high' WHEN c.age >= 41 THEN 'mid' ELSE 'low' END;
