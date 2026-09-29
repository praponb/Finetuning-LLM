SELECT store_id, COUNT(*) total_count
FROM inventory
GROUP BY store_id
ORDER BY store_id;
