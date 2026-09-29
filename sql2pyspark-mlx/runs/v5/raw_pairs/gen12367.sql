SELECT we.duration_sec, s.started_at, c.signup_date
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
JOIN customers c ON c.id = s.customer_id
ORDER BY started_at DESC;
