SELECT id, country
FROM suppliers
WHERE country IN ('TH', 'US') AND rating >= 4.1
ORDER BY country, id DESC
LIMIT 20
