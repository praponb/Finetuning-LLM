SELECT status, customer_id FROM orders WHERE order_date >= '2025-07-04' EXCEPT SELECT status, customer_id FROM orders WHERE channel IN ('mobile', 'store');
