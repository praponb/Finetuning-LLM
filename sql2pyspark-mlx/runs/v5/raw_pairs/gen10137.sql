SELECT s.id, s.store_name, COUNT(i.id) AS inventory_count
FROM stores s
LEFT JOIN inventory i ON s.id = i.store_id
WHERE s.sqft BETWEEN 8129 AND 10787
GROUP BY s.id, s.store_name
ORDER BY id DESC;
