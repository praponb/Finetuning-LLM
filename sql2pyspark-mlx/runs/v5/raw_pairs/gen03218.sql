SELECT p.category, MAX(oi.unit_price) max_unit_price
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
GROUP BY p.category
ORDER BY category
