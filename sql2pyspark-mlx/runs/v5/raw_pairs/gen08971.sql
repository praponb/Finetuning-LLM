WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER w_grp AS amount_bucket,
        LEAD(discount, 1) OVER (ORDER BY order_date, id) AS next_discount,
        NTILE(4) OVER (PARTITION BY status, customer_id ORDER BY shipping_fee, id) AS shipping_fee_bucket,
        LAST_VALUE(channel) IGNORE NULLS OVER w_grp_2 AS last_known_channel
    FROM orders
    WINDOW w_grp AS (ORDER BY amount DESC, id), w_grp_2 AS (ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest_match AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_id,
        s.order_date,
        s.status,
        s.channel,
        s.amount,
        s.shipping_fee,
        s.next_discount,
        s.last_known_channel,
        latest.shipment_status
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.status AS shipment_status
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date <= s.order_date
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        shipping_fee,
        shipment_status,
        DENSE_RANK() OVER (PARTITION BY shipment_status, status ORDER BY next_discount DESC NULLS LAST) AS next_discount_dense_rank,
        LAG(next_discount, 1) OVER (PARTITION BY store_id ORDER BY shipping_fee, id) AS prev_next_discount,
        LAG(shipping_fee, 2) OVER (PARTITION BY status, shipment_status ORDER BY order_date, id) AS prev_shipping_fee,
        ROW_NUMBER() OVER w_part AS row_seq,
        LEAD(amount, 2) OVER (PARTITION BY status, channel ORDER BY shipping_fee, id) AS next_amount
    FROM latest_match
    WINDOW w_part AS (ORDER BY next_discount DESC, id)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND MONTH(x.shipped_date) IN (1, 4, 6)
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.product_id IN (13, 34, 37, 39)
        )
)
SELECT
    id,
    order_date,
    status,
    channel,
    shipping_fee,
    shipment_status,
    prev_next_discount,
    row_seq,
    next_amount
FROM not_flagged;
