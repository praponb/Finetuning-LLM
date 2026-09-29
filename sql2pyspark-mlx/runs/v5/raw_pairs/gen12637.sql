WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        amount - shipping_fee AS amount_with_shipping,
        amount * discount AS amount_times_discount,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM orders
    WHERE customer_id IS NOT NULL
      AND amount < 1330
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, WEEKOFYEAR(order_date) AS order_date_weekofyear
    FROM valid
    WHERE order_date >= DATE '2023-09-18'
      AND channel IS NOT NULL
),
valid_orders AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        discount * shipping_fee AS discount_times_shipping_fee,
        CASE WHEN amount >= 1690 THEN 'High' WHEN amount >= 440 THEN 'Medium' ELSE 'Low' END AS amount_tier,
        COALESCE(channel, 'DIRECT') AS resolved_channel_2
    FROM filtered
    WHERE channel IS NOT NULL
),
with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.status,
        s.channel,
        s.shipping_fee,
        s.amount_with_shipping,
        s.amount_times_discount,
        s.discount_times_shipping_fee,
        s.amount_tier,
        s.resolved_channel_2,
        x.payment_status,
        x.payment_paid_at
    FROM valid_orders s
    LEFT JOIN LATERAL (
            SELECT c.status AS payment_status, c.paid_at AS payment_paid_at
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.payment_paid_at
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        store_id,
        payment_status,
        discount_times_shipping_fee
    FROM eligible
) src
PIVOT (
    MAX(discount_times_shipping_fee)
    FOR payment_status IN ('REFUNDED' AS refunded, 'SUCCESS' AS success, 'FAILED' AS failed)
);
