SELECT s.city, MAX(i.quantity) max_quantity
FROM inventory i
JOIN products p ON i.product_id = p.id
JOIN stores s ON i.store_id = s.id
GROUP BY s.city
ORDER BY s.city, max_quantity
