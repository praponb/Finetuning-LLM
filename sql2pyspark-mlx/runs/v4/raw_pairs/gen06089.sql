SELECT s.city, COUNT(i.id) AS inventory_count, MAX(i.last_restock) AS latest_last_restock FROM stores s LEFT JOIN inventory i ON i.store_id = s.id WHERE i.product_id IN (2, 7) GROUP BY s.city
