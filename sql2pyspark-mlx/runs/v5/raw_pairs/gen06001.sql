WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        cost,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date DESC, id) AS seq_no,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id) AS seq_no_2
    FROM shipments
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(cost * 0.9, 2) AS adjusted_cost,
        COALESCE(cost_share, 0) AS cost_share_filled
    FROM enriched
    WHERE seq_no != 3
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.cost,
        src.cost_share,
        src.adjusted_cost,
        b.status,
        b.amount
    FROM base src
    LEFT JOIN orders b
        ON src.order_id <=> b.id
    WHERE b.store_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        cost_share,
        adjusted_cost,
        LAST_VALUE(cost_share, TRUE) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost_share,
        AVG(cost) OVER (PARTITION BY status) AS status_avg_cost
    FROM with_dims
),
base_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        adjusted_cost,
        last_known_cost_share,
        ROUND(cost_share / status_avg_cost, 2) AS cost_share_per_status_avg_cost
    FROM scored
    WHERE cost_share < 0.42
),
scored_shipments AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(adjusted_cost) OVER (ORDER BY adjusted_cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_adjusted_cost,
        SUM(adjusted_cost) OVER (ORDER BY adjusted_cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_adjusted_cost,
        COUNT(adjusted_cost) OVER (ORDER BY adjusted_cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_adjusted_cost
    FROM base_shipments
)
SELECT
    id,
    adjusted_cost,
    last7_min_adjusted_cost,
    last7_sum_adjusted_cost,
    last7_count_adjusted_cost
FROM scored_shipments
