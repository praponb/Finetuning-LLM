SELECT
    e.*,
    b.device,
    b.browser,
    b.country
FROM web_events e
LEFT JOIN sessions b
    ON e.session_id = b.id;
