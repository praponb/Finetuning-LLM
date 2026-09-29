SELECT
    we.session_id,
    s.started_at,
    c.email,
    c.segment
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
FULL OUTER JOIN customers c ON s.customer_id = c.id
WHERE we.duration_sec > 398 OR c.city NOT IN ('Singapore', 'Tokyo', 'New York')
ORDER BY c.email, started_at
