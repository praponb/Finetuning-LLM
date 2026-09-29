SELECT unit_price AS value, 'order_item' AS source
FROM order_items
WHERE product_id = 39
UNION
SELECT shipping_fee AS value, 'order' AS source
FROM orders;
