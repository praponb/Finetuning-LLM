SELECT p.id, p.brand, COUNT(i.id) AS inventory_count
FROM products p
LEFT OUTER JOIN inventory i ON p.id = i.product_id
GROUP BY p.id, p.brand;
