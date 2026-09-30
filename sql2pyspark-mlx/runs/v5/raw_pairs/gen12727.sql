SELECT *, FLOOR(quantity) quantity_whole
FROM order_items
WHERE unit_price BETWEEN 300 AND 340
ORDER BY order_id;
