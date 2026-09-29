SELECT
    c.country,
    DATE_TRUNC('MONTH', c.signup_date) AS signup_month,
    COUNT(CASE WHEN we.page = '/home' THEN 1 END) AS home_count,
    MAX(we.event_time) AS last_event_time
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON c.id = s.customer_id
WHERE s.customer_id = 27
GROUP BY c.country, DATE_TRUNC('MONTH', c.signup_date);
