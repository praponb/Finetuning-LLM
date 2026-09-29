SELECT o.shipping_fee, o.discount, oi.unit_price FROM orders o JOIN order_items oi ON o.id = oi.order_id WHERE o.status IN ('PENDING', 'COMPLETED') OR o.store_id = 27;
