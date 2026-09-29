SELECT id, country, COUNT(*) OVER (PARTITION BY country) AS country_count
FROM suppliers
WHERE country IN ('DE', 'US')
ORDER BY id DESC;
