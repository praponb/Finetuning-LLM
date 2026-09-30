SELECT
    id,
    method,
    paid_at,
    amount,
    DATE_FORMAT(paid_at, 'MMM yyyy') AS paid_at_label
FROM payments
WHERE HOUR(paid_at) BETWEEN 8 AND 14
ORDER BY id;
