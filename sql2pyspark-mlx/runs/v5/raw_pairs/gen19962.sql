SELECT s.id, s.browser
FROM sessions s
WHERE EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id AND we.page <> '/product')
ORDER BY s.id DESC
