SELECT
    we.duration_sec,
    we.session_id,
    s.device,
    s.started_at,
    s.browser,
    CONCAT(s.country, ' ', s.device) AS country_device
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE s.country = 'US';
