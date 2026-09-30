SELECT id, order_id, paid_at
FROM (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY paid_at) AS pos
    FROM payments
    WHERE status IN ('FAILED', 'REFUNDED', 'SUCCESS')
) AS r
WHERE pos = 1;
