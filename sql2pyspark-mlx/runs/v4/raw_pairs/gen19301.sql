SELECT store_id, COUNT(DISTINCT quantity) unique_stores
FROM inventory
GROUP BY 1
