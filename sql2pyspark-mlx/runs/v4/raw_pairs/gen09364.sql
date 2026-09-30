SELECT *
FROM payments
WHERE order_id IS NULL OR status = 'FAILED';
