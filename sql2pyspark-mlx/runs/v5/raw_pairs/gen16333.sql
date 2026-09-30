SELECT event_type, SUM(CASE WHEN page = '/checkout' THEN 1 ELSE 0 END) AS checkout_count
FROM web_events
GROUP BY event_type
ORDER BY event_type DESC, checkout_count ASC
LIMIT 10;
