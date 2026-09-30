SELECT s.id, COUNT(i.id) AS inventory_count
FROM stores s
LEFT OUTER JOIN inventory i ON s.id = i.store_id
WHERE s.opened_date >= '2023-09-04'
GROUP BY s.id
HAVING COUNT(i.id) >= 4
ORDER BY inventory_count, id;
