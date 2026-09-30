SELECT order_id, method FROM payments WHERE method NOT IN ('bank_transfer', 'cash') EXCEPT SELECT order_id, method FROM payments WHERE order_id IS NOT NULL;
