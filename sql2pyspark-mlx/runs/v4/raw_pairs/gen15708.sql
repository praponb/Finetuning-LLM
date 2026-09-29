SELECT p.category, COUNT(i.quantity) AS quantity_count
FROM inventory i
RIGHT JOIN products p ON i.product_id = p.id
GROUP BY p.category
HAVING COUNT(i.quantity) >= 1;
