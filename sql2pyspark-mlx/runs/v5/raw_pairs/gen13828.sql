WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        discount * shipping_fee AS discount_times_shipping_fee,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM orders
    WHERE order_date > DATE '2023-09-04'
      AND shipping_fee < 26
),
with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.payment_status
    FROM valid s
    JOIN LATERAL (
            SELECT c.status AS payment_status
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.order_id = 16
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        store_id,
        status,
        payment_status,
        RANK() OVER (PARTITION BY status ORDER BY discount NULLS LAST) AS discount_rank,
        LAG(discount_times_shipping_fee, 1) OVER (PARTITION BY payment_status, customer_id ORDER BY discount, id) AS prev_discount_times_shipping_fee,
        RANK() OVER (ORDER BY discount_times_shipping_fee DESC) AS discount_times_shipping_fee_rank
    FROM kept
    WHERE customer_id IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN prev_discount_times_shipping_fee >= 75 THEN 'LARGE' ELSE 'SMALL' END AS prev_discount_times_shipping_fee_tier,
    ROUND(SUM(discount_rank) / NULLIF(COUNT(CASE WHEN status = 'SHIPPED' THEN 1 END), 0), 2) AS discount_rank_per_shipped,
    COUNT_IF(payment_status = 'REFUNDED') AS refunded_events
FROM enriched
GROUP BY CASE WHEN prev_discount_times_shipping_fee >= 75 THEN 'LARGE' ELSE 'SMALL' END
HAVING ROUND(SUM(discount_rank) / NULLIF(COUNT(CASE WHEN status = 'SHIPPED' THEN 1 END), 0), 2) > 0
ORDER BY refunded_events DESC;
