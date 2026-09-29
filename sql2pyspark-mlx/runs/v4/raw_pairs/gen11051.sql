SELECT s.city, COUNT(i.id) inventory_count, COUNT(i.quantity) quantity_count
FROM stores s
LEFT JOIN inventory i ON i.store_id = s.id
GROUP BY s.city;
