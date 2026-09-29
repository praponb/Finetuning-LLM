SELECT id, shipping_fee, customer_id, store_id, amount + shipping_fee AS amount_with_shipping FROM orders WHERE status = 'CANCELLED';
