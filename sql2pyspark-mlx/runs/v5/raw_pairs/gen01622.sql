SELECT 'order_item' AS record_type, unit_price AS value
FROM order_items
WHERE quantity <= 3
UNION
SELECT 'order' AS record_type, shipping_fee AS value
FROM orders
WHERE status = 'RETURNED'
ORDER BY record_type;
