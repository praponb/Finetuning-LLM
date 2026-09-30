SELECT status FROM payments WHERE amount > 590 EXCEPT SELECT status FROM shipments ORDER BY status
