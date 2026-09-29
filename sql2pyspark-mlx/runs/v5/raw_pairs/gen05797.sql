WITH with_dims AS (
    -- Attach products attributes
    SELECT
        e.*,
        b.cost,
        b.price,
        b.stock_qty
    FROM order_items e
    LEFT JOIN products b
        ON e.product_id = b.id
    WHERE b.category <> 'Grocery'
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.quantity,
        s.cost,
        s.stock_qty,
        p.stock_qty AS product_stock_qty
    FROM with_dims s
    INNER JOIN products p
        ON s.product_id = p.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        stock_qty,
        COUNT(quantity) OVER (PARTITION BY product_id ORDER BY product_stock_qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_quantity,
        MIN(quantity) OVER (PARTITION BY product_id ORDER BY product_stock_qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY product_stock_qty, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_quantity,
        RANK() OVER (PARTITION BY product_id ORDER BY cost DESC) AS cost_rank
    FROM enriched
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        last3_sum_quantity / NULLIF(stock_qty, 0) AS last3_sum_quantity_per_stock_qty,
        CASE WHEN stock_qty >= 441 THEN 'High' WHEN stock_qty >= 137 THEN 'Medium' ELSE 'Low' END AS stock_qty_level
    FROM metrics
    WHERE cost_rank BETWEEN 6 AND 8
),
enriched_order_items AS (
    -- Attach products attributes
    SELECT s.*, d.category
    FROM cleaned s
    LEFT JOIN products d
        ON s.product_id = d.id
),
metrics_order_items AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(stock_qty) OVER (PARTITION BY category ORDER BY last3_count_quantity, id) AS prev_stock_qty,
        LEAD(stock_qty) OVER (PARTITION BY category ORDER BY last3_count_quantity, id) AS next_stock_qty,
        LEAD(last3_count_quantity) OVER (PARTITION BY category ORDER BY last3_count_quantity, id) AS next_last3_count_quantity,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY last3_count_quantity, id) AS seq_no,
        FIRST_VALUE(last3_sum_quantity_per_stock_qty, TRUE) OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last3_sum_quantity_per_stock_qty
    FROM enriched_order_items
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        stock_qty,
        last3_min_quantity,
        last3_sum_quantity_per_stock_qty,
        stock_qty_level,
        prev_stock_qty,
        AVG(cost_rank) OVER (PARTITION BY product_id ORDER BY first_known_last3_sum_quantity_per_stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_cost_rank,
        MIN(cost_rank) OVER (PARTITION BY product_id ORDER BY first_known_last3_sum_quantity_per_stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_cost_rank,
        MAX(cost_rank) OVER (PARTITION BY product_id ORDER BY first_known_last3_sum_quantity_per_stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_cost_rank,
        COUNT(cost_rank) OVER (PARTITION BY product_id ORDER BY first_known_last3_sum_quantity_per_stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_cost_rank,
        STDDEV_SAMP(last3_sum_quantity_per_stock_qty) OVER (PARTITION BY category) AS category_stddev_samp_last3_sum_quantity_per_stock_qty
    FROM metrics_order_items
),
grouped AS (
    -- Grouped metrics
    SELECT
        product_id,
        CASE WHEN category_stddev_samp_last3_sum_quantity_per_stock_qty >= 67 THEN 'LARGE' ELSE 'SMALL' END AS category_stddev_samp_last3_sum_quantity_per_stock_qty_tier,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(last7_max_cost_rank, ARRAY(0.25, 0.5, 0.75)) AS last7_max_cost_rank_quartiles
    FROM with_windows
    GROUP BY GROUPING SETS ((product_id, CASE WHEN category_stddev_samp_last3_sum_quantity_per_stock_qty >= 67 THEN 'LARGE' ELSE 'SMALL' END), (product_id), ())
)
SELECT row_count
FROM grouped
ORDER BY row_count DESC;
