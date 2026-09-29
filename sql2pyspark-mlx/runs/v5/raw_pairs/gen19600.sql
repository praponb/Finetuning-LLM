SELECT
    e.id,
    e.session_id,
    s.country,
    s.browser,
    s.device
FROM web_events e
INNER JOIN sessions s
    ON e.session_id = s.id
WHERE s.device IN ('tablet', 'desktop');
