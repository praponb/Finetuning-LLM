SELECT
    id,
    order_id,
    status,
    paid_at,
    COUNT(*) OVER (PARTITION BY order_id) AS order_id_count,
    NTILE(2) OVER (PARTITION BY status ORDER BY paid_at, id) AS bucket
FROM payments
WHERE method NOT IN ('bank_transfer', 'card')
