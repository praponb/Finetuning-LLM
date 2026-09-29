WITH base AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(carrier, 'N/A') AS carrier_filled
    FROM shipments
    WHERE order_id IS NOT NULL
      AND carrier IS DISTINCT FROM 'Kerry'
),
kept_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM base
    EXCEPT
    SELECT status, id
    FROM shipments
    WHERE status != 'IN_TRANSIT'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.status = k.status AND s.id = k.id
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.shipped_date,
        s.cost,
        s.status,
        s.carrier_filled,
        d.discount,
        d.shipping_fee
    FROM verified s
    JOIN orders d
        ON s.order_id <=> d.id
    WHERE d.shipping_fee >= 23
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(shipping_fee) AS shipping_fee_whole
    FROM joined
    WHERE order_id IS NOT NULL
      AND shipping_fee <= 12
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY shipping_fee_whole DESC NULLS FIRST) AS shipping_fee_whole_rank,
        STDDEV_SAMP(shipping_fee) OVER (PARTITION BY order_id) AS order_id_stddev_samp_shipping_fee,
        MAX(shipping_fee) OVER (PARTITION BY status) AS status_max_shipping_fee
    FROM filtered
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY shipping_fee DESC NULLS LAST) AS shipping_fee_dense_rank,
        DENSE_RANK() OVER w_hist AS shipping_fee_whole_rank_dense_rank,
        STDDEV_SAMP(shipping_fee_whole) OVER w_recent AS order_id_stddev_samp_shipping_fee_whole,
        STDDEV_SAMP(order_id_stddev_samp_shipping_fee) OVER w_recent AS order_id_stddev_samp_order_id_stddev_samp_shipping_fee
    FROM metrics
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY shipping_fee_whole_rank NULLS FIRST), w_recent AS (PARTITION BY order_id)
),
filtered_shipments AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        shipped_date,
        cost,
        shipping_fee,
        shipping_fee_whole,
        order_id_stddev_samp_shipping_fee,
        status_max_shipping_fee,
        shipping_fee_dense_rank,
        shipping_fee_whole_rank_dense_rank,
        order_id_stddev_samp_shipping_fee_whole,
        COALESCE(shipping_fee_whole, 0) AS shipping_fee_whole_filled
    FROM scored
    WHERE shipped_date >= DATE '2023-04-06'
      AND order_id IS NOT NULL
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN order_id_stddev_samp_shipping_fee_whole >= 68 THEN 'LARGE' ELSE 'SMALL' END AS order_id_stddev_samp_shipping_fee_whole_tier,
        APPROX_PERCENTILE(shipping_fee_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS shipping_fee_dense_rank_quartiles,
        AVG(shipping_fee_whole) AS avg_shipping_fee_whole,
        ROUND(AVG(order_id_stddev_samp_shipping_fee_whole), 2) AS avg_order_id_stddev_samp_shipping_fee_whole,
        PERCENTILE_APPROX(shipping_fee_whole_filled, ARRAY(0.25, 0.5, 0.75)) AS shipping_fee_whole_filled_quartiles,
        ROUND(AVG(cost), 2) AS avg_cost
    FROM filtered_shipments
    GROUP BY CASE WHEN order_id_stddev_samp_shipping_fee_whole >= 68 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    order_id_stddev_samp_shipping_fee_whole_tier,
    shipping_fee_dense_rank_quartiles,
    avg_shipping_fee_whole,
    shipping_fee_whole_filled_quartiles
FROM totals
ORDER BY order_id_stddev_samp_shipping_fee_whole_tier DESC NULLS LAST, avg_shipping_fee_whole
