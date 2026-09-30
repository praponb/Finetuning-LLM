WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        last_restock,
        LAST_DAY(last_restock) AS last_restock_month_end,
        COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE product_id = 7
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.last_restock,
        e.last_restock_month_end,
        e.quantity_filled,
        b.city
    FROM cleaned e
    INNER JOIN stores b
        ON e.store_id = b.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        last_restock_month_end,
        quantity_filled,
        city,
        NTILE(4) OVER (ORDER BY quantity_filled, id) AS quantity_filled_bucket,
        LEAD(quantity_filled, 2) OVER (ORDER BY last_restock_month_end, id) AS next_quantity_filled,
        LAG(quantity_filled, 1) OVER (PARTITION BY city ORDER BY last_restock, id) AS prev_quantity_filled,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock_month_end DESC, id) AS seq_no,
        SUM(quantity_filled) OVER (PARTITION BY city ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_filled
    FROM with_dims
),
scored_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        prev_quantity_filled,
        AVG(running_quantity_filled) OVER w_ordered AS running_avg_running_quantity_filled,
        SUM(running_quantity_filled) OVER w_ordered AS running_sum_running_quantity_filled,
        COUNT(running_quantity_filled) OVER w_ordered AS running_count_running_quantity_filled,
        DENSE_RANK() OVER (ORDER BY next_quantity_filled DESC NULLS FIRST) AS next_quantity_filled_dense_rank
    FROM scored
    WINDOW w_ordered AS (PARTITION BY city ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(prev_quantity_filled) OVER (ORDER BY next_quantity_filled_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_prev_quantity_filled,
        MIN(prev_quantity_filled) OVER (ORDER BY next_quantity_filled_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_prev_quantity_filled,
        COUNT(prev_quantity_filled) OVER (ORDER BY next_quantity_filled_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_prev_quantity_filled
    FROM scored_inventory
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(running_sum_running_quantity_filled, 1) OVER (ORDER BY running_count_running_quantity_filled, id) AS next_running_sum_running_quantity_filled,
        LAG(running_max_prev_quantity_filled, 2) OVER w_grp AS prev_running_max_prev_quantity_filled,
        FIRST_VALUE(running_avg_running_quantity_filled, TRUE) OVER w_running AS first_known_running_avg_running_quantity_filled,
        SUM(next_quantity_filled_dense_rank) OVER (ORDER BY running_count_running_quantity_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_quantity_filled_dense_rank,
        ROW_NUMBER() OVER (ORDER BY running_min_prev_quantity_filled DESC, id) AS seq_no
    FROM metrics
    WHERE running_max_prev_quantity_filled < 48
    WINDOW w_grp AS (ORDER BY prev_quantity_filled, id), w_running AS (ORDER BY running_min_prev_quantity_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, running_min_prev_quantity_filled / NULLIF(running_avg_running_quantity_filled, 0) AS running_min_prev_quantity_filled_per_running_avg_running_quantity_filled
    FROM with_windows
    WHERE running_count_prev_quantity_filled < 13
)
SELECT
    id,
    prev_quantity_filled,
    running_avg_running_quantity_filled,
    running_sum_running_quantity_filled,
    running_count_running_quantity_filled,
    running_max_prev_quantity_filled,
    running_min_prev_quantity_filled,
    running_count_prev_quantity_filled,
    next_running_sum_running_quantity_filled
FROM valid
