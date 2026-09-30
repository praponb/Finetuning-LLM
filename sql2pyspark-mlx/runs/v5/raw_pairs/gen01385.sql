SELECT s.id, s.cost, o.shipping_fee, o.customer_id FROM shipments s JOIN orders o ON s.order_id = o.id;
