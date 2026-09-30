SELECT status FROM orders WHERE store_id IS NULL EXCEPT SELECT status FROM payments ORDER BY status;
