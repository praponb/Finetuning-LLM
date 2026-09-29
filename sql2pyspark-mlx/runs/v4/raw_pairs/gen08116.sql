SELECT
    s.*,
    b.browser,
    b.device,
    b.country
FROM web_events s
LEFT JOIN sessions b
    ON s.session_id = b.id
