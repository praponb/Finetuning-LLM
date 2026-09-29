SELECT
    status,
    method,
    CASE WHEN amount >= 1560 THEN 'high' WHEN amount >= 220 THEN 'mid' ELSE 'low' END amount_bucket,
    MAX(amount) max_amount,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) success_count
FROM payments
GROUP BY status, method, CASE WHEN amount >= 1560 THEN 'high' WHEN amount >= 220 THEN 'mid' ELSE 'low' END
HAVING max_amount >= 970
