SELECT id, store_id, order_date FROM (SELECT id, store_id, order_date, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY order_date DESC, id) AS rn FROM orders) AS ranked WHERE rn = 1
