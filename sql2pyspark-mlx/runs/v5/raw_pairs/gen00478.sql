SELECT 'order_item' AS kind, unit_price
FROM order_items
UNION ALL
SELECT 'payment' AS kind, amount AS unit_price
FROM payments;
