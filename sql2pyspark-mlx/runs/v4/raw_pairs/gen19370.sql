SELECT s.region, MAX(i.quantity) - MIN(i.quantity) quantity_range
FROM inventory i
LEFT OUTER JOIN stores s ON i.store_id = s.id
WHERE i.product_id IN (2, 3, 4)
GROUP BY s.region
HAVING MAX(i.quantity) - MIN(i.quantity) >= 34
