SELECT id, store_id, shipping_fee, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY shipping_fee DESC, id) row_num FROM orders WHERE status = 'CANCELLED';
