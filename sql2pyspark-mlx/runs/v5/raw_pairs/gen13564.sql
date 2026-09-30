SELECT order_id, unit_price, product_id
FROM order_items
WHERE unit_price BETWEEN 150 AND 220;
