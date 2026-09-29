SELECT id, store_id, order_date FROM orders WHERE amount BETWEEN 900 AND 1530 AND channel IS NOT NULL;
