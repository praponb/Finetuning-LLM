SELECT id, method FROM payments EXCEPT SELECT id, method FROM payments WHERE order_id = 39;
