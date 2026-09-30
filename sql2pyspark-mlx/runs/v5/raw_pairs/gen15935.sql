SELECT
    s.id,
    s.country,
    c.city,
    c.email
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id;
