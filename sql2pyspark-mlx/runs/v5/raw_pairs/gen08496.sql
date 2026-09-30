SELECT id, method, status, CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END AS is_refunded FROM payments WHERE amount BETWEEN 800 AND 1110;
