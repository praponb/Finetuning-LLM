SELECT
    id,
    status,
    amount,
    paid_at
FROM (
    SELECT
        id,
        status,
        amount,
        paid_at,
        RANK() OVER (PARTITION BY status ORDER BY amount) AS rn
    FROM payments
    WHERE order_id IN (15, 16)
) AS t
WHERE rn <= 3
ORDER BY id DESC
