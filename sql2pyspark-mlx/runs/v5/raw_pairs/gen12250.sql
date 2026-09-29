SELECT we.duration_sec, s.device, s.country
FROM web_events we
FULL OUTER JOIN sessions s ON s.id = we.session_id
WHERE s.device = 'desktop' AND s.customer_id IN (3, 18, 19, 40)
ORDER BY country, device DESC
