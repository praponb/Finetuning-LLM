SELECT
    we.event_time,
    s.customer_id,
    s.started_at,
    c.city,
    c.country,
    TRIM(c.city) clean_city
FROM web_events we
FULL OUTER JOIN sessions s ON we.session_id = s.id
LEFT JOIN customers c ON c.id = s.customer_id
WHERE c.age <= 31
ORDER BY customer_id;
