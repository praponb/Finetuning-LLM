SELECT
    we.page,
    we.event_time,
    s.country,
    s.browser,
    c.country AS customer_country
FROM web_events we
JOIN sessions s ON s.id = we.session_id
JOIN customers c ON s.customer_id = c.id
WHERE we.event_time < '2023-12-01'
ORDER BY s.country DESC;
