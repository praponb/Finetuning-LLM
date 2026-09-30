SELECT s.customer_id, SUM(CASE WHEN we.page = '/checkout' THEN 1 ELSE 0 END) AS checkout_count, AVG(we.duration_sec) AS avg_duration_sec
FROM web_events we
JOIN sessions s ON we.session_id = s.id
WHERE s.browser = 'Chrome'
GROUP BY s.customer_id;
