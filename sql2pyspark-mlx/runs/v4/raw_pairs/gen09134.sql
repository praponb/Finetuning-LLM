SELECT s.city, ROUND(AVG(i.quantity), 2) AS avg_quantity
FROM inventory i
JOIN products p ON p.id = i.product_id
LEFT JOIN stores s ON i.store_id = s.id
GROUP BY s.city
ORDER BY avg_quantity DESC, city
LIMIT 10;
