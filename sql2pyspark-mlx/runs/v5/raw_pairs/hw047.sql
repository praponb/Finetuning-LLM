SELECT id, order_date, LEAD(order_date, 1) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS next_order_date FROM orders;
