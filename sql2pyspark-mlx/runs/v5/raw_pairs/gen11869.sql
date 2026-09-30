WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        CASE WHEN quantity >= 9 THEN 'High' WHEN quantity >= 4 THEN 'Medium' ELSE 'Low' END AS quantity_level
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY quantity DESC, id ASC) AS quantity_bucket,
        LAG(quantity, 1) OVER (PARTITION BY order_id ORDER BY quantity ASC, id ASC) AS prev_quantity
    FROM base
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY quantity_bucket ASC, id ASC) AS row_num
    FROM with_windows
    QUALIFY row_num = 1
),
enriched AS (
    -- Attach products attributes
    SELECT s.*, p.stock_qty
    FROM latest s
    JOIN products p
        ON s.product_id = p.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(prev_quantity, quantity) AS least_prev_quantity_quantity
    FROM enriched
    WHERE product_id IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        quantity_level,
        prev_quantity,
        row_num,
        LEAD(quantity) OVER w_grp AS next_quantity,
        LEAD(prev_quantity) OVER w_grp AS next_prev_quantity,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY quantity ASC, id ASC)
)
-- Grouped metrics
SELECT
    order_id,
    CASE WHEN next_quantity >= 8 THEN 'HIGH' ELSE 'LOW' END AS next_quantity_tier,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    PERCENTILE_APPROX(prev_quantity, ARRAY(0.25, 0.5, 0.75)) AS prev_quantity_quartiles,
    SUM(quantity) AS total_quantity,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    PERCENTILE_APPROX(next_quantity, ARRAY(0.25, 0.5, 0.75)) AS next_quantity_quartiles
FROM scored
GROUP BY 1, 2
