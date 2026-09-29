SELECT o.discount, o.status, p.status AS payment_status, p.order_id FROM orders o LEFT JOIN payments p ON p.order_id = o.id WHERE o.shipping_fee >= 16;
