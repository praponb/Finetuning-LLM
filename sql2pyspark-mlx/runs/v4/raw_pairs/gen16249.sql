SELECT
    method,
    status,
    order_id,
    MIN(amount) AS min_amount,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count
FROM payments
GROUP BY 1, 2, 3
ORDER BY method DESC, status, order_id
LIMIT 5;
