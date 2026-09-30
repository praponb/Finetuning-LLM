WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        shipped_date,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY cost DESC) AS cost_cume_dist
    FROM shipments
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.channel,
        b.shipping_fee
    FROM enriched src
    JOIN orders b
        ON src.order_id <=> b.id
    WHERE b.shipping_fee <= 20
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched_shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost_share DESC, id) = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        cost_share / NULLIF(shipping_fee, 0) AS cost_share_per_shipping_fee,
        cost_share * shipping_fee AS cost_share_times_shipping_fee,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM latest
    WHERE shipped_date >= DATE '2023-07-16'
),
matched_keys AS (
    -- Set difference on keys
    SELECT order_id
    FROM base
    EXCEPT
    SELECT order_id
    FROM shipments
    WHERE carrier IN ('UPS', 'Flash')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN matched_keys k ON s.order_id = k.order_id
)
-- Multi-level aggregation
SELECT
    CASE WHEN cost_share_times_shipping_fee >= 83 THEN 'HIGH' ELSE 'LOW' END AS cost_share_times_shipping_fee_tier,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    ROUND(AVG(cost_share), 2) AS avg_cost_share,
    APPROX_PERCENTILE(cost_share, 0.95) AS cost_share_p95
FROM verified
GROUP BY CASE WHEN cost_share_times_shipping_fee >= 83 THEN 'HIGH' ELSE 'LOW' END;
