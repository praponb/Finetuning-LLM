SELECT store_id, order_date FROM orders WHERE channel IN ('partner', 'web') EXCEPT SELECT store_id, order_date FROM orders WHERE status IN ('RETURNED', 'COMPLETED') ORDER BY order_date;
