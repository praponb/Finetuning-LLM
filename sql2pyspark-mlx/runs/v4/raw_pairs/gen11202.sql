SELECT order_items.product_id, orders.channel FROM order_items JOIN orders ON order_items.order_id = orders.id
