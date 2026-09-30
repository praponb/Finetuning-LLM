SELECT p.brand, MAX(i.last_restock) AS latest_last_restock, MIN(i.quantity) AS min_quantity
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
GROUP BY p.brand
ORDER BY min_quantity DESC, brand
LIMIT 20;
