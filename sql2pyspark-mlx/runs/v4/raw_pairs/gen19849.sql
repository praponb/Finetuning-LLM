SELECT s.city, MAX(oi.unit_price) AS max_unit_price
FROM order_items oi
FULL OUTER JOIN products p ON oi.product_id = p.id
RIGHT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.cost BETWEEN 270 AND 560
GROUP BY s.city
ORDER BY s.city DESC;
