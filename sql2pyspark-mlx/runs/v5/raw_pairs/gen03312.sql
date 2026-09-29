SELECT
    c.id,
    c.city,
    COUNT(s.id) AS session_count,
    COUNT(CASE WHEN s.browser = 'Safari' THEN 1 END) AS safari_count
FROM customers c
LEFT JOIN sessions s ON s.customer_id = c.id
GROUP BY c.id, c.city
HAVING safari_count > 4;
