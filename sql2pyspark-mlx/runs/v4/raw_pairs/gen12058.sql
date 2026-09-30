SELECT
    status,
    method,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
    COUNT(amount) AS amount_count
FROM payments
GROUP BY ROLLUP(status, method)
ORDER BY status
