WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN carrier IN ('Flash', 'FedEx') THEN 'Tier 1' ELSE 'Other' END carrier_group,
        COALESCE(carrier, 'NONE') resolved_carrier
    FROM shipments
    WHERE delivered_date > TO_DATE('2024-01-11')
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.shipping_fee,
        o.discount,
        o.channel
    FROM filtered src
    JOIN orders o
        ON src.order_id = o.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT src.*, b.shipping_fee AS order_shipping_fee
    FROM joined src
    JOIN orders b
        ON src.order_id = b.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(order_shipping_fee, 2) OVER w_ordered prev_order_shipping_fee,
        SUM(shipping_fee) OVER w_recent cumulative_shipping_fee
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY carrier ORDER BY shipping_fee, id), w_recent AS (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        carrier_group,
        resolved_carrier,
        shipping_fee,
        discount,
        channel,
        order_shipping_fee,
        prev_order_shipping_fee,
        cumulative_shipping_fee,
        DENSE_RANK() OVER (PARTITION BY carrier, order_id ORDER BY shipping_fee) shipping_fee_dense_rank,
        RANK() OVER (ORDER BY order_shipping_fee DESC NULLS FIRST) order_shipping_fee_rank
    FROM scored
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY delivered_date, id) event_seq,
        NTILE(10) OVER (PARTITION BY status ORDER BY cumulative_shipping_fee DESC, id) cumulative_shipping_fee_bucket
    FROM metrics
)
-- Window metrics over several different partitions
SELECT
    *,
    MAX(order_shipping_fee) OVER (PARTITION BY channel, order_id) channel_max_order_shipping_fee,
    AVG(cumulative_shipping_fee) OVER (PARTITION BY status, order_id ORDER BY shipped_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) cumulative_shipping_fee_avg_3_prior,
    COUNT(cost) OVER (PARTITION BY carrier) carrier_count_cost
FROM enriched;
