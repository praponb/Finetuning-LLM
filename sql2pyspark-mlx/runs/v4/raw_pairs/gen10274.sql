SELECT customer_id FROM orders WHERE order_date <= '2025-04-01' EXCEPT SELECT customer_id FROM orders WHERE shipping_fee <= 24 ORDER BY customer_id;
