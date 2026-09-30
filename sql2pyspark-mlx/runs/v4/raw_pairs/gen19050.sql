SELECT amount, 'order' AS source
FROM orders
UNION
SELECT unit_price AS amount, 'order_item' AS source
FROM order_items
ORDER BY source DESC
