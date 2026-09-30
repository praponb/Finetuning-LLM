SELECT AVG(sqft) avg_sqft
FROM stores
WHERE opened_date < '2025-05-27' OR sqft IS NOT NULL
ORDER BY avg_sqft DESC;
