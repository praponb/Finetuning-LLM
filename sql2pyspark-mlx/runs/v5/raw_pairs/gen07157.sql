SELECT
    s.country,
    c.is_active,
    c.city,
    c.segment
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id
ORDER BY segment ASC
