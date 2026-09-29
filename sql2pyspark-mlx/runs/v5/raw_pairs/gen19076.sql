SELECT
    s.id,
    s.browser,
    s.started_at,
    s.customer_id,
    c.age,
    c.city,
    c.is_active,
    LENGTH(c.country) AS country_length
FROM sessions s
JOIN customers c ON c.id = s.customer_id
WHERE c.is_active AND c.email LIKE '%@yahoo.com';
