SELECT s.city, ROUND(AVG(i.quantity), 2) avg_quantity, MIN(i.quantity) min_quantity
FROM inventory i
RIGHT OUTER JOIN products p ON i.product_id = p.id
JOIN suppliers s ON s.id = p.supplier_id
WHERE p.brand = 'Initech'
GROUP BY s.city;
