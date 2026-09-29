SELECT
    id,
    method,
    paid_at,
    ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at, id) AS rn
FROM payments
WHERE order_id = 23
ORDER BY id DESC
