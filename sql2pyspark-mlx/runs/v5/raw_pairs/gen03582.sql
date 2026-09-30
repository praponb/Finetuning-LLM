SELECT
    id,
    paid_at,
    method,
    DATE_FORMAT(paid_at, 'MMM yyyy') AS paid_at_label
FROM payments
WHERE method IS NOT NULL
ORDER BY paid_at;
