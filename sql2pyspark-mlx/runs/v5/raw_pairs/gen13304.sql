SELECT p.brand, SUM(i.quantity) AS total_quantity
FROM inventory i
JOIN stores s ON i.store_id = s.id
JOIN products p ON p.id = i.product_id
GROUP BY p.brand
HAVING SUM(i.quantity) > 380
ORDER BY brand;
