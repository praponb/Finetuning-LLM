SELECT o.id, o.status, o.shipping_fee, o.order_date FROM orders o WHERE o.shipping_fee = (SELECT MAX(o2.shipping_fee) FROM orders o2 WHERE o2.status = o.status);
