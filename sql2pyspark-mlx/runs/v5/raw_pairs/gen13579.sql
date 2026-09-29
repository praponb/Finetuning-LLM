SELECT id, quantity, unit_price FROM order_items WHERE quantity = (SELECT MIN(quantity) FROM order_items);
