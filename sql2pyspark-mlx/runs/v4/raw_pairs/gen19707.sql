SELECT order_items.unit_price, order_items.order_id, orders.amount FROM order_items INNER JOIN orders ON order_items.order_id = orders.id;
