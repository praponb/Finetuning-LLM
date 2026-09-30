SELECT
    s.id,
    s.device,
    s.browser,
    s.country,
    c.is_active,
    c.country AS customer_country,
    SUBSTRING(s.country, 1, 2) AS country_prefix
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE c.age BETWEEN 33 AND 59;
