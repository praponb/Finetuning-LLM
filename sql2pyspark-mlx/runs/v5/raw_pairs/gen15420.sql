SELECT i.store_id, s.region
FROM inventory i
LEFT OUTER JOIN stores s ON i.store_id = s.id
WHERE i.last_restock > '2023-10-01';
