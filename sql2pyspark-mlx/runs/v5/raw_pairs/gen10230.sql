SELECT s.order_id, o.discount, o.shipping_fee FROM shipments s FULL OUTER JOIN orders o ON s.order_id = o.id
