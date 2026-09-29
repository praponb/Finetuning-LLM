WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        LENGTH(method) AS method_length,
        COALESCE(method, 'OTHER') AS method_clean
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-11-08 01:00:00'
      AND order_id IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.shipping_fee,
        d.amount AS order_amount,
        d.discount
    FROM base e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.*,
        o.shipping_fee AS order_shipping_fee,
        o.channel
    FROM enriched e
    INNER JOIN orders o
        ON e.order_id = o.id
    WHERE o.channel NOT IN ('partner', 'mobile')
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(order_shipping_fee, 0) AS order_shipping_fee_rounded,
        COALESCE(method, 'DIRECT') AS method_filled
    FROM with_dims
    WHERE paid_at >= TO_DATE('2023-05-19')
      AND order_id IS NOT NULL
)
-- Join a dimension and aggregate
SELECT
    o.status,
    e.channel AS payment_channel,
    COUNT(*) AS rows_total,
    MAX(e.amount) AS max_amount,
    ROUND(SUM(e.order_shipping_fee), 2) AS total_order_shipping_fee
FROM valid e
INNER JOIN orders o ON e.order_id = o.id
GROUP BY o.status, e.channel
ORDER BY total_order_shipping_fee NULLS FIRST
