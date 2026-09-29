WITH base AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(channel)) AS channel_title
    FROM orders
    WHERE customer_id IS NOT NULL
      AND shipping_fee BETWEEN 25 AND 32
)
-- Latest matching payment per row (LATERAL ... LIMIT 1)
SELECT
    s.id,
    s.store_id,
    s.status,
    s.amount,
    s.shipping_fee,
    s.channel_title,
    latest.payment_paid_at
FROM base s
JOIN LATERAL (
        SELECT c.paid_at AS payment_paid_at
        FROM payments c
        WHERE c.order_id = s.id
        ORDER BY c.amount DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE
