SELECT
    c.country,
    c.city,
    COUNT(DISTINCT s.country) AS unique_countrys,
    SUM(CASE WHEN s.country = 'JP' THEN 1 ELSE 0 END) AS jp_count
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY c.country, c.city
ORDER BY city DESC
