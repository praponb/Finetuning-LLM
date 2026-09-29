WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(status, ' ', carrier) AS status_carrier
    FROM shipments
    WHERE delivered_date >= DATE '2024-02-02'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        status_carrier,
        SUM(cost) OVER w_part AS running_cost,
        NTILE(5) OVER (ORDER BY cost DESC, id) AS cost_bucket,
        MIN(cost) OVER w_running AS order_id_min_cost
    FROM base
    WINDOW w_part AS (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY order_id)
),
with_windows_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        status_carrier,
        running_cost,
        cost_bucket,
        order_id_min_cost,
        LEAD(cost_bucket, 2) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS next_cost_bucket,
        SUM(cost_bucket) OVER (PARTITION BY status, order_id ORDER BY cost_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_bucket,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share,
        SUM(order_id_min_cost) OVER (PARTITION BY status, carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_order_id_min_cost
    FROM with_windows
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.shipping_fee,
        b.amount
    FROM with_windows_shipments src
    INNER JOIN orders b
        ON src.order_id = b.id
),
with_dims_shipments AS (
    -- Attach orders attributes
    SELECT
        a.*,
        d.shipping_fee AS order_shipping_fee,
        d.status AS order_status
    FROM with_dims a
    INNER JOIN orders d
        ON a.order_id = d.id
),
base_shipments AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        shipped_date,
        cost,
        status_carrier,
        running_cost,
        cost_bucket,
        order_id_min_cost,
        cumulative_cost_bucket,
        cost_share,
        order_shipping_fee,
        order_status,
        CAST(cumulative_cost_bucket AS STRING) AS cumulative_cost_bucket_str,
        COALESCE(next_cost_bucket, 0) AS next_cost_bucket_filled
    FROM with_dims_shipments
    WHERE delivered_date > DATE '2024-03-24'
)
SELECT
    id,
    order_id,
    status_carrier,
    cost_bucket,
    order_shipping_fee,
    order_status,
    next_cost_bucket_filled,
    CASE WHEN order_status RLIKE '^(PENDING|RETURNED)$' THEN 'GROUP_A' WHEN order_status RLIKE '^(COMPLETED|CANCELLED)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS order_status_zone
FROM base_shipments
ORDER BY order_status DESC NULLS LAST, order_status_zone;
