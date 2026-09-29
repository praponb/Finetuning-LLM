SELECT p.id, p.category, COUNT(i.id) AS inventory_count
FROM products p
JOIN inventory i ON i.product_id = p.id
GROUP BY p.id, p.category
ORDER BY p.id, inventory_count DESC;
