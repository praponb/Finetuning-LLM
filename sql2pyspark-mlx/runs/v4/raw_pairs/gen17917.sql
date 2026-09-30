SELECT s.city, COUNT(i.quantity) AS quantity_count, MIN(i.last_restock) AS first_last_restock FROM inventory i JOIN stores s ON i.store_id = s.id GROUP BY s.city ORDER BY first_last_restock, city;
