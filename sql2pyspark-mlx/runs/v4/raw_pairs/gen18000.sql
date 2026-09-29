SELECT
    status,
    method,
    MIN(paid_at) AS first_paid_at,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count
FROM payments
GROUP BY status, method
ORDER BY failed_count DESC
