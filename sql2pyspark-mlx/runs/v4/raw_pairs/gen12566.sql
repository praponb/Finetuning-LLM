SELECT id, store_id, order_date, NTILE(4) OVER (PARTITION BY store_id ORDER BY order_date DESC, id) AS bucket FROM orders WHERE store_id = 1 ORDER BY store_id DESC;
