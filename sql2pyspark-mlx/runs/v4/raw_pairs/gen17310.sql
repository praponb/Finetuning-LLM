SELECT we.page, s.browser, s.device
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
