SELECT p.supplier_id, COUNT(DISTINCT oi.unit_price) AS unique_orders
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
INNER JOIN orders o ON oi.order_id = o.id
GROUP BY p.supplier_id;
