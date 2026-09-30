SELECT
    amount_bucket,
    method,
    failed_count,
    min_amount
FROM (
    SELECT
        CASE WHEN amount >= 1560 THEN 'high' WHEN amount >= 460 THEN 'mid' ELSE 'low' END amount_bucket,
        method,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) failed_count,
        MIN(amount) min_amount
    FROM payments
    WHERE order_id IS NOT NULL
    GROUP BY CASE WHEN amount >= 1560 THEN 'high' WHEN amount >= 460 THEN 'mid' ELSE 'low' END, method
) agg
WHERE min_amount > 1110
ORDER BY amount_bucket
