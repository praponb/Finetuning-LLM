SELECT status FROM orders EXCEPT SELECT status FROM payments WHERE amount < 1220 ORDER BY status;
