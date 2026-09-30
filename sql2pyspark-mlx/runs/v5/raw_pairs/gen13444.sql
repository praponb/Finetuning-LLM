SELECT
    CASE WHEN amount >= 1370 THEN 'high' WHEN amount >= 560 THEN 'mid' ELSE 'low' END AS amount_bucket,
    method,
    AVG(amount) AS avg_amount,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
    COUNT(amount) AS amount_count
FROM payments
GROUP BY 1, 2
HAVING avg_amount >= 1510
ORDER BY avg_amount DESC, amount_count DESC
