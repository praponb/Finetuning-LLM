SELECT order_date, customer_id, CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END AS is_cancelled FROM orders WHERE shipping_fee <= 37 ORDER BY is_cancelled, order_date DESC
