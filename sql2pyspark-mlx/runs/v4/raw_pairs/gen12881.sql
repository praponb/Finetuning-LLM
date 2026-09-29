SELECT
    s.id,
    s.started_at,
    c.segment,
    c.email
FROM sessions s
RIGHT JOIN customers c ON c.id = s.customer_id
WHERE NOT (c.country IN ('UK', 'DE') OR TO_DATE(s.started_at) >= '2025-10-01');
