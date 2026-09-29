SELECT
    s.id,
    s.browser,
    s.customer_id,
    s.country
FROM sessions s
WHERE s.country = 'UK' AND NOT EXISTS (SELECT 1 FROM web_events we WHERE we.session_id = s.id)
