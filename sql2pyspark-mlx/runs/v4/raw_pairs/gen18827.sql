SELECT order_items.id, order_items.order_id, order_items.unit_price, orders.amount FROM order_items LEFT JOIN orders ON order_items.order_id = orders.id
