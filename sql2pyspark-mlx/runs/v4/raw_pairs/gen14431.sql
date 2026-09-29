SELECT o.id, o.order_id, o.quantity, o.unit_price FROM order_items o WHERE o.quantity = (SELECT MIN(o2.quantity) FROM order_items o2 WHERE o2.order_id = o.order_id);
