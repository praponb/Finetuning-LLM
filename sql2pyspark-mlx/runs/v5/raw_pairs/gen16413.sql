SELECT id, quantity, unit_price
FROM order_items
WHERE quantity >= (SELECT AVG(quantity) FROM order_items);
