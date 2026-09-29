SELECT amount, shipping_fee, store_id
FROM orders
WHERE channel IN ('mobile', 'store', 'web');
