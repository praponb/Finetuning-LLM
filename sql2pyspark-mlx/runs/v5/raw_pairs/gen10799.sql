SELECT order_items.id, order_items.unit_price FROM order_items WHERE order_items.order_id IN (SELECT id FROM orders WHERE MONTH(order_date) IN (2, 7, 12));
