SELECT p.brand, SUM(oi.quantity * oi.unit_price) AS revenue, ROUND(STDDEV(oi.quantity), 2) AS stddev_quantity
FROM order_items oi
RIGHT JOIN products p ON oi.product_id = p.id
WHERE p.supplier_id IN (15, 33, 38)
GROUP BY p.brand
ORDER BY revenue, 1
LIMIT 1;
