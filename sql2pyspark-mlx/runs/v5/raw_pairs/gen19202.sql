SELECT id, quantity FROM order_items WHERE quantity > (SELECT AVG(quantity) FROM order_items) ORDER BY quantity DESC;
