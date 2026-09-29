WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        shipping_fee,
        FLOOR(shipping_fee) AS shipping_fee_whole,
        CASE WHEN amount >= 1560 THEN 'High' WHEN amount >= 640 THEN 'Medium' ELSE 'Low' END AS amount_level
    FROM orders
    WHERE order_date > TO_DATE('2023-03-04')
      AND channel = 'store'
),
latest_match AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.channel,
        s.amount,
        s.shipping_fee,
        x.payment_method
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.method AS payment_method
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.status = 'RETURNED'
        )
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.amount,
        s.shipping_fee,
        top1.paid_at
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.payment_paid_at
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.paid_at AS payment_paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    amount,
    shipping_fee,
    paid_at,
    payment_paid_at
FROM with_last
