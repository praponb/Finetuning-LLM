SELECT
    s.id,
    s.device,
    c.name,
    c.email
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE s.country IN ('TH', 'SG', 'UK') OR c.segment = 'Corporate'
ORDER BY name DESC;
