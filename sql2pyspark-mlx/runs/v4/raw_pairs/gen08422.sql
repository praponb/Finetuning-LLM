SELECT s.id, s.region, COUNT(i.id) inventory_count
FROM stores s
LEFT JOIN inventory i ON s.id = i.store_id
WHERE s.sqft IS NULL
GROUP BY s.id, s.region
ORDER BY s.region, id
LIMIT 1;
