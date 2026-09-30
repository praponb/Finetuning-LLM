SELECT CASE WHEN c.age >= 56 THEN 'high' WHEN c.age >= 31 THEN 'mid' ELSE 'low' END AS age_bucket, MIN(we.duration_sec) AS min_duration_sec
FROM web_events we
LEFT JOIN sessions s ON s.id = we.session_id
LEFT JOIN customers c ON s.customer_id = c.id
GROUP BY CASE WHEN c.age >= 56 THEN 'high' WHEN c.age >= 31 THEN 'mid' ELSE 'low' END
HAVING MIN(we.duration_sec) > 250
ORDER BY 1;
