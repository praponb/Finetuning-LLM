SELECT
    status,
    CASE WHEN amount >= 1390 THEN 'high' WHEN amount >= 330 THEN 'mid' ELSE 'low' END AS amount_bucket,
    SUM(CASE WHEN status = 'FAILED' THEN amount ELSE 0 END) AS failed_amount,
    MIN(paid_at) AS min_paid_at,
    COUNT(amount) AS amount_count
FROM payments
WHERE status NOT IN ('FAILED', 'REFUNDED')
GROUP BY CUBE(status, CASE WHEN amount >= 1390 THEN 'high' WHEN amount >= 330 THEN 'mid' ELSE 'low' END);
