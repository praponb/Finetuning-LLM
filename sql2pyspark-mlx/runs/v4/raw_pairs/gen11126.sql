SELECT status
FROM payments
WHERE amount BETWEEN 830 AND 1110
UNION
SELECT status
FROM payments
WHERE status = 'REFUNDED';
