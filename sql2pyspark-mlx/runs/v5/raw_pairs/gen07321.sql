SELECT CASE WHEN c.credit_limit >= 17200 THEN 'high' WHEN c.credit_limit >= 3700 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, AVG(we.duration_sec) AS avg_duration_sec
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY CASE WHEN c.credit_limit >= 17200 THEN 'high' WHEN c.credit_limit >= 3700 THEN 'mid' ELSE 'low' END
HAVING AVG(we.duration_sec) > 340
ORDER BY credit_limit_bucket, avg_duration_sec DESC
