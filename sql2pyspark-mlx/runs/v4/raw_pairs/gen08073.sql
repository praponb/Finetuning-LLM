SELECT id, amount, method, CONCAT(status, ', ', method) AS status_method FROM payments WHERE order_id IN (3, 11)
