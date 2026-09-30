SELECT s.city, MIN(i.last_restock) AS first_last_restock FROM inventory i LEFT JOIN stores s ON s.id = i.store_id GROUP BY s.city ORDER BY city;
