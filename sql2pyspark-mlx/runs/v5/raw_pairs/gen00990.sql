SELECT o.id, o.channel, o.shipping_fee FROM orders o WHERE o.id IN (SELECT order_id FROM payments WHERE method IN ('paypal', 'promptpay', 'cash'))
