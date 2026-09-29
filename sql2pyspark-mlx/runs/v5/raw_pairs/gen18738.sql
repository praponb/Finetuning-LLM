SELECT id, order_id FROM payments EXCEPT SELECT id, order_id FROM payments WHERE method <> 'paypal';
