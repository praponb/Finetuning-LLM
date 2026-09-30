SELECT
    method,
    status,
    COUNT(*) AS total_count,
    ROUND(AVG(amount), 2) AS avg_amount
FROM payments
WHERE amount <= 1330
GROUP BY method, status
HAVING SUM(CASE WHEN method = 'bank_transfer' THEN 1 ELSE 0 END) > 59
ORDER BY method, status
LIMIT 3
