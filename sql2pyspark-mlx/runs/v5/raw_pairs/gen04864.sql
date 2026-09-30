WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS dedup_rank
    FROM shipments
    QUALIFY dedup_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        status,
        STDDEV_SAMP(cost) OVER (PARTITION BY carrier) AS carrier_stddev_samp_cost,
        PERCENT_RANK() OVER (ORDER BY cost DESC NULLS LAST) AS cost_percent_rank,
        RANK() OVER (PARTITION BY carrier, order_id ORDER BY cost NULLS LAST) AS cost_rank
    FROM ranked
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        d.shipping_fee,
        d.order_date,
        d.status AS order_status
    FROM with_windows s
    LEFT JOIN orders d
        ON s.order_id = d.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY order_date DESC NULLS LAST, id) AS rank_in_group
    FROM with_dims
    QUALIFY rank_in_group = 1
)
-- Join a dimension and aggregate
SELECT
    o.status AS order_status,
    o.channel,
    e.order_status AS shipment_order_status,
    COUNT(*) AS rows_total,
    MIN(e.carrier_stddev_samp_cost) AS min_carrier_stddev_samp_cost,
    GROUPING_ID() AS level_id
FROM latest e
INNER JOIN orders o ON e.order_id = o.id
GROUP BY GROUPING SETS ((o.status, o.channel, e.order_status), (o.status), ())
ORDER BY channel DESC NULLS FIRST
