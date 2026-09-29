WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        FLOOR(cost) AS cost_whole,
        CASE WHEN status IN ('RETURNED', 'IN_TRANSIT') THEN 'Group A' ELSE 'Other' END AS status_group,
        COALESCE(carrier, 'NONE') AS carrier_clean
    FROM shipments
    WHERE order_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        delivered_date,
        cost_whole,
        status_group,
        SUM(cost) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_cost,
        AVG(cost) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_cost,
        COUNT(cost) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_cost,
        MAX(cost) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_cost,
        RANK() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS FIRST) AS cost_rank
    FROM cleaned
    WHERE carrier IN ('UPS', 'FedEx', 'Kerry')
),
enriched AS (
    -- Attach orders attributes
    SELECT a.*, b.discount
    FROM scored a
    INNER JOIN orders b
        ON a.order_id = b.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.last7_count_cost,
        e.last7_max_cost,
        b.discount AS order_discount,
        b.shipping_fee,
        b.order_date
    FROM enriched e
    LEFT JOIN orders b
        ON e.order_id <=> b.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last7_count_cost,
        last7_max_cost,
        order_discount,
        shipping_fee,
        SUM(order_discount) OVER (PARTITION BY order_id ORDER BY last7_max_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_order_discount,
        DENSE_RANK() OVER (ORDER BY last7_count_cost NULLS FIRST) AS last7_count_cost_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date, id) AS row_seq
    FROM with_dims
),
enriched_shipments AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(running_order_discount) OVER w_running AS last4_sum_running_order_discount,
        MIN(running_order_discount) OVER w_running AS last4_min_running_order_discount,
        MAX(running_order_discount) OVER w_running AS last4_max_running_order_discount
    FROM metrics
    WINDOW w_running AS (ORDER BY last7_count_cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
scored_shipments AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(last7_max_cost, 1) OVER (ORDER BY row_seq, id) AS prev_last7_max_cost,
        LEAD(last4_sum_running_order_discount, 2) OVER (ORDER BY last4_sum_running_order_discount, id) AS next_last4_sum_running_order_discount
    FROM enriched_shipments
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN shipping_fee >= 41 THEN 'HIGH' ELSE 'LOW' END AS shipping_fee_tier,
        PERCENTILE_APPROX(order_discount, ARRAY(0.25, 0.5, 0.75)) AS order_discount_quartiles,
        APPROX_PERCENTILE(row_seq, 0.5) AS row_seq_p50,
        APPROX_PERCENTILE(last4_min_running_order_discount, ARRAY(0.25, 0.5, 0.75)) AS last4_min_running_order_discount_quartiles,
        COUNT(DISTINCT id) AS distinct_ids
    FROM scored_shipments
    GROUP BY 1
)
SELECT
    shipping_fee_tier,
    row_seq_p50,
    distinct_ids
FROM stats
ORDER BY shipping_fee_tier DESC NULLS LAST, row_seq_p50 DESC;
