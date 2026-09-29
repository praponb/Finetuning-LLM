WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        ROUND(unit_price * 0.85, 2) AS adjusted_unit_price
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT s.*, o.status
    FROM valid s
    LEFT JOIN orders o
        ON s.order_id = o.id
    WHERE o.store_id = 14
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY adjusted_unit_price DESC, id) AS rn
    FROM with_dims
    QUALIFY rn = 1
),
with_dims_order_items AS (
    -- Attach products attributes
    SELECT s.*, p.launch_date
    FROM deduped s
    LEFT JOIN products p
        ON s.product_id <=> p.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_dims_order_items
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY quantity NULLS FIRST) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        quantity,
        adjusted_unit_price,
        status,
        rn,
        SUM(quantity) OVER w_recent AS running_sum_quantity,
        MAX(quantity) OVER w_recent AS running_max_quantity,
        COUNT(quantity) OVER w_recent AS running_count_quantity,
        MIN(quantity) OVER w_recent AS running_min_quantity
    FROM latest
    WINDOW w_recent AS (PARTITION BY product_id, status ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    quantity,
    adjusted_unit_price,
    status,
    running_sum_quantity,
    running_max_quantity,
    running_count_quantity,
    running_min_quantity,
    CASE WHEN status RLIKE '^(CANCELLED|SHIPPED)$' THEN 'GROUP_A' WHEN status RLIKE '^(COMPLETED|PENDING)$' THEN 'CORE' ELSE 'GROUP_B' END AS status_zone
FROM enriched
ORDER BY running_count_quantity DESC NULLS LAST
