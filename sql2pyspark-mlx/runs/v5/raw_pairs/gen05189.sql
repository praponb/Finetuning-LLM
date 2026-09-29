SELECT CASE WHEN c.credit_limit >= 17500 THEN 'high' WHEN c.credit_limit >= 7800 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, MIN(s.started_at) AS min_started_at
FROM sessions s
JOIN customers c ON s.customer_id = c.id
GROUP BY CASE WHEN c.credit_limit >= 17500 THEN 'high' WHEN c.credit_limit >= 7800 THEN 'mid' ELSE 'low' END
ORDER BY credit_limit_bucket, min_started_at DESC;
