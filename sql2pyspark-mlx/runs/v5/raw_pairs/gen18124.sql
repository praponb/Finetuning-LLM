SELECT orders.store_id, order_items.unit_price, order_items.unit_price - order_items.quantity AS line_total
FROM orders
INNER JOIN order_items ON orders.id = order_items.order_id
