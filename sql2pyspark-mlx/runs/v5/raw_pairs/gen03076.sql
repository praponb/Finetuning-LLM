SELECT we.id, we.duration_sec
FROM web_events we
WHERE we.session_id IN (SELECT id FROM sessions WHERE country IN ('DE', 'US', 'JP'))
ORDER BY duration_sec DESC
