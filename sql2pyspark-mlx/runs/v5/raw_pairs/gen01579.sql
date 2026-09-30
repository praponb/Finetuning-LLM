WITH enriched AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.carrier,
        a.delivered_date,
        a.status,
        d.shipping_fee
    FROM shipments a
    INNER JOIN orders d
        ON a.order_id <=> d.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, carrier
    FROM enriched
    EXCEPT ALL
    SELECT status, carrier
    FROM shipments
    WHERE shipped_date > '2025-04-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.carrier = k.carrier
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        a.order_id,
        a.carrier,
        a.delivered_date,
        a.shipping_fee,
        o.status AS order_status,
        o.channel
    FROM reconciled a
    LEFT JOIN orders o
        ON a.order_id = o.id
    WHERE o.amount BETWEEN 430 AND 870
),
enriched_shipments_2 AS (
    -- Window metrics over several different partitions
    SELECT
        order_id,
        order_status,
        channel,
        MIN(shipping_fee) OVER (PARTITION BY order_status) AS order_status_min_shipping_fee,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY shipping_fee DESC NULLS FIRST) AS shipping_fee_cume_dist
    FROM enriched_shipments
)
SELECT
    order_status,
    order_status_min_shipping_fee,
    shipping_fee_cume_dist
FROM enriched_shipments_2
ORDER BY order_status_min_shipping_fee NULLS FIRST, shipping_fee_cume_dist;
