SELECT we.session_id, s.country
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
WHERE s.browser = 'Chrome'
ORDER BY session_id DESC
