SELECT order_date, store_id, shipping_fee FROM orders WHERE channel NOT IN ('store', 'web');
