SELECT
    c.country,
    c.age,
    s.country AS session_country,
    s.customer_id,
    s.device,
    LOWER(c.city) AS city_lower
FROM customers c
LEFT JOIN sessions s ON c.id = s.customer_id
ORDER BY 4 DESC
