SELECT * FROM (SELECT status, method, amount FROM payments WHERE status IN ('SUCCESS', 'FAILED', 'REFUNDED'))
PIVOT (MAX(amount) FOR method IN ('paypal', 'card'))
ORDER BY status;
