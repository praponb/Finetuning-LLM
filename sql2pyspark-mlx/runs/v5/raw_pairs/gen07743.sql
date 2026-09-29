SELECT id, store_id, customer_id FROM orders WHERE YEAR(order_date) >= 2024 EXCEPT SELECT id, store_id, customer_id FROM orders WHERE discount <= 0.16;
