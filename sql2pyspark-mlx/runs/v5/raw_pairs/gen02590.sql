SELECT price, 'product' AS kind
FROM products
UNION
SELECT unit_price, 'order_item' AS kind
FROM order_items
WHERE order_id = 27;
