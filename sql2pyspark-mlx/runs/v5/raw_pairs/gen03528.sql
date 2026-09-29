SELECT
    id,
    method,
    CONCAT_WS(',', method, status) method_status_key,
    ROUND(amount, 0) amount_rounded
FROM payments
WHERE status IN ('SUCCESS', 'REFUNDED', 'FAILED') AND paid_at < '2024-04-25'
ORDER BY method_status_key, amount_rounded DESC, id
LIMIT 20
