SELECT 'order_item' AS origin, unit_price
FROM order_items
WHERE order_id IN (7, 10, 25, 28)
UNION
SELECT 'order' AS origin, shipping_fee AS unit_price
FROM orders
ORDER BY unit_price;
