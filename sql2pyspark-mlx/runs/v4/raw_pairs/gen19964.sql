SELECT status, MAX(order_date) last_order_date, MIN(order_date) first_order_date FROM orders WHERE customer_id IN (21, 22, 26, 29) GROUP BY status
