SELECT s.region, COUNT(i.id) AS inventory_count, MAX(i.last_restock) AS latest_last_restock
FROM stores s
LEFT JOIN inventory i ON i.store_id = s.id
GROUP BY s.region
ORDER BY region, inventory_count DESC;
