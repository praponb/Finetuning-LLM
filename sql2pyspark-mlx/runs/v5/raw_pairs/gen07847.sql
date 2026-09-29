SELECT
    status,
    order_id,
    CASE WHEN method IN ('bank_transfer', 'promptpay') THEN 'tier_1' ELSE 'other' END AS method_group,
    ROUND((100 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_failed,
    MIN(amount) AS min_amount
FROM payments
GROUP BY status, order_id, method_group
HAVING COUNT(amount) > 2
ORDER BY min_amount DESC, status, order_id, method_group
LIMIT 10
