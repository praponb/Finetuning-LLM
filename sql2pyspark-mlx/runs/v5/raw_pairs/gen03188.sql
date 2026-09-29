WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        ROUND(unit_price, 1) AS unit_price_rounded,
        CAST(quantity AS STRING) AS quantity_num
    FROM order_items
    WHERE quantity BETWEEN 4 AND 7
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        quantity,
        unit_price_rounded,
        quantity_num,
        SUM(unit_price_rounded) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_unit_price_rounded,
        COUNT(unit_price_rounded) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_unit_price_rounded,
        MIN(unit_price_rounded) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_unit_price_rounded,
        AVG(unit_price_rounded) OVER (PARTITION BY product_id ORDER BY unit_price_rounded, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS unit_price_rounded_avg_3
    FROM base
    WHERE unit_price_rounded IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(running_min_unit_price_rounded, 1) AS running_min_unit_price_rounded_rounded,
        unit_price_rounded_avg_3 * unit_price_rounded AS unit_price_rounded_avg_3_times_unit_price_rounded
    FROM metrics
    WHERE unit_price_rounded_avg_3 <= 29
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        unit_price_rounded,
        running_sum_unit_price_rounded,
        running_min_unit_price_rounded,
        unit_price_rounded_avg_3,
        running_min_unit_price_rounded_rounded,
        LEAD(quantity) OVER (ORDER BY running_count_unit_price_rounded, id) AS next_quantity,
        LAG(quantity) OVER (ORDER BY running_count_unit_price_rounded, id) AS prev_quantity,
        LAG(running_min_unit_price_rounded) OVER (ORDER BY running_count_unit_price_rounded, id) AS prev_running_min_unit_price_rounded,
        LEAD(running_min_unit_price_rounded) OVER (ORDER BY running_count_unit_price_rounded, id) AS next_running_min_unit_price_rounded,
        ROW_NUMBER() OVER (ORDER BY running_count_unit_price_rounded, id) AS row_seq,
        NTILE(10) OVER (ORDER BY unit_price_rounded_avg_3_times_unit_price_rounded, id) AS unit_price_rounded_avg_3_times_unit_price_rounded_bucket
    FROM filtered
),
filtered_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        quantity,
        running_sum_unit_price_rounded,
        unit_price_rounded_avg_3,
        running_min_unit_price_rounded_rounded,
        next_quantity,
        prev_running_min_unit_price_rounded,
        next_running_min_unit_price_rounded,
        row_seq,
        LEAST(unit_price_rounded, prev_running_min_unit_price_rounded) AS least_unit_price_rounded_prev_running_min_unit_price_rounded
    FROM enriched
    WHERE running_min_unit_price_rounded BETWEEN 45 AND 59
)
SELECT
    id,
    quantity,
    running_sum_unit_price_rounded,
    unit_price_rounded_avg_3,
    running_min_unit_price_rounded_rounded,
    next_quantity,
    prev_running_min_unit_price_rounded,
    next_running_min_unit_price_rounded,
    row_seq
FROM filtered_order_items
ORDER BY next_running_min_unit_price_rounded DESC NULLS LAST;
