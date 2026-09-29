WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, quantity + unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND unit_price > 160
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        line_total
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price NULLS LAST, id) AS rank_in_group
        FROM cleaned
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (ORDER BY line_total DESC, id) AS line_total_bucket,
        AVG(line_total) OVER w_running AS line_total_avg_5
    FROM latest
    WINDOW w_running AS (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, line_total_bucket - unit_price AS line_total_bucket_minus_unit_price
    FROM with_windows
    WHERE line_total_avg_5 <> 67
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(line_total_bucket) OVER w_running AS running_sum_line_total_bucket,
        MIN(line_total_bucket) OVER w_running AS running_min_line_total_bucket,
        AVG(line_total_bucket) OVER w_running AS running_avg_line_total_bucket,
        COUNT(line_total_bucket) OVER w_running AS running_count_line_total_bucket
    FROM valid
    WHERE quantity < 5
    WINDOW w_running AS (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Join a dimension and aggregate
SELECT
    p.category,
    COUNT(*) AS num_rows,
    ROUND(AVG(a.quantity), 2) AS avg_quantity,
    MAX(a.unit_price) AS max_unit_price
FROM enriched a
INNER JOIN products p ON a.product_id = p.id
GROUP BY CUBE (p.category);
