SELECT p.category, AVG(i.quantity) avg_quantity
FROM inventory i
JOIN products p ON p.id = i.product_id
WHERE i.quantity BETWEEN 163 AND 226
GROUP BY p.category
