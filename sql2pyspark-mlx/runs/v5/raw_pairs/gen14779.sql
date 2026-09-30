SELECT c.country, COUNT(*) AS total_count
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
LEFT JOIN customers c ON s.customer_id = c.id
GROUP BY c.country;
