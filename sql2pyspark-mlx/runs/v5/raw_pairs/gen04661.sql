WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        FLOOR(quantity) quantity_whole
    FROM order_items
    WHERE quantity < 3
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        product_id,
        quantity_whole
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_whole NULLS LAST, id) = 1
),
deduped_order_items AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        quantity_whole
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity_whole DESC, id) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        quantity_whole,
        AVG(quantity_whole) OVER w_part last4_avg_quantity_whole,
        SUM(quantity_whole) OVER w_part last4_sum_quantity_whole,
        MAX(quantity_whole) OVER w_part last4_max_quantity_whole
    FROM deduped_order_items
    WINDOW w_part AS (PARTITION BY order_id ORDER BY quantity_whole, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.status,
        b.channel
    FROM metrics src
    INNER JOIN orders b
        ON src.order_id = b.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN last4_sum_quantity_whole >= 390 THEN 'PREMIUM' ELSE 'STANDARD' END AS last4_sum_quantity_whole_tier,
    CASE WHEN last4_max_quantity_whole >= 60 THEN 'HIGH' ELSE 'LOW' END AS last4_max_quantity_whole_tier,
    order_id,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending_count,
    COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count,
    APPROX_PERCENTILE(last4_sum_quantity_whole, 0.95) AS last4_sum_quantity_whole_p95,
    PERCENTILE_APPROX(last4_sum_quantity_whole, 0.95) AS last4_sum_quantity_whole_p95_2,
    AVG(last4_sum_quantity_whole) FILTER (WHERE status = 'PENDING') AS pending_avg_last4_sum_quantity_whole,
    APPROX_PERCENTILE(last4_avg_quantity_whole, 0.9) AS last4_avg_quantity_whole_p90
FROM enriched
GROUP BY CASE WHEN last4_sum_quantity_whole >= 390 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last4_max_quantity_whole >= 60 THEN 'HIGH' ELSE 'LOW' END, order_id
HAVING last4_avg_quantity_whole_p90 > 0
