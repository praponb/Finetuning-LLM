SELECT id, country, COUNT(*) OVER (PARTITION BY country) AS country_count
FROM suppliers
