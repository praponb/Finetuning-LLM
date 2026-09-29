WITH joined AS (
    SELECT e.*, b.shipping_fee
    FROM shipments e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
with_windows AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        shipping_fee,
        AVG(cost) OVER (PARTITION BY order_id ORDER BY delivered_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cost_avg_2_prior,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share,
        SUM(shipping_fee) OVER (PARTITION BY carrier ORDER BY shipped_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee,
        NTILE(4) OVER (PARTITION BY carrier ORDER BY shipping_fee DESC, id ASC) AS shipping_fee_bucket,
        SUM(cost) OVER (PARTITION BY order_id ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost
    FROM joined
),
with_dims AS (
    SELECT
        e.*,
        o.shipping_fee AS order_shipping_fee,
        o.status AS order_status,
        o.order_date
    FROM with_windows e
    LEFT JOIN orders o
        ON e.order_id = o.id
),
with_dims_shipments AS (
    SELECT
        src.*,
        d.channel,
        d.shipping_fee AS order_shipping_fee_2,
        d.order_date AS order_order_date
    FROM with_dims src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
stats AS (
    SELECT
        DATE_TRUNC('WEEK', shipped_date) AS week_start,
        CASE WHEN cost_avg_2_prior >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_avg_2_prior_tier,
        COUNT(CASE WHEN order_status = 'CANCELLED' THEN 1 END) AS cancelled_count,
        COUNT(*) FILTER (WHERE order_status = 'CANCELLED') AS cancelled_rows,
        SUM(CASE WHEN status = 'IN_TRANSIT' THEN cost_avg_2_prior ELSE 0 END) AS in_transit_cost_avg_2_prior,
        COUNT(DISTINCT status) AS distinct_status,
        GROUPING_ID() AS gid
    FROM with_dims_shipments
    GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', shipped_date), CASE WHEN cost_avg_2_prior >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('WEEK', shipped_date)))
)
SELECT
    week_start,
    cancelled_count,
    cancelled_rows,
    in_transit_cost_avg_2_prior,
    gid
FROM stats
ORDER BY gid DESC, cancelled_count DESC;
