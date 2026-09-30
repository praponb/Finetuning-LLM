SELECT id, quantity, unit_price FROM order_items WHERE quantity >= (SELECT MAX(quantity) FROM order_items) ORDER BY quantity, id DESC
