SELECT p.paid_at, o.store_id, o.channel, o.shipping_fee FROM payments p JOIN orders o ON p.order_id = o.id ORDER BY channel ASC;
