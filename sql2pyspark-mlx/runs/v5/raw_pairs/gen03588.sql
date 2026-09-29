WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at >= s.order_date
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
        )
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        channel,
        amount,
        shipping_fee
    FROM not_flagged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, status ORDER BY amount DESC NULLS LAST, id) = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        channel,
        amount,
        shipping_fee,
        CASE WHEN channel IN ('mobile', 'store') THEN 'Core' ELSE 'Other' END AS channel_group,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM deduped
    WHERE customer_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        channel,
        amount,
        channel_clean,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY amount DESC NULLS LAST) AS amount_dense_rank,
        AVG(shipping_fee) OVER (ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_3,
        ROUND(amount / SUM(amount) OVER (PARTITION BY channel), 4) AS amount_share
    FROM base
),
with_latest AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.cost,
        top1.delivered_date
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.cost, c.delivered_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest_orders AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.amount,
        s.shipping_fee_avg_3,
        s.amount_share,
        s.cost,
        s.delivered_date,
        x.shipment_delivered_date
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.delivered_date AS shipment_delivered_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN amount_share >= 0.88 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_share_tier,
        COUNT(*) AS event_count,
        MAX(amount) AS max_amount
    FROM with_latest_orders
    GROUP BY 1
)
SELECT
    amount_share_tier,
    event_count,
    max_amount
FROM totals;
