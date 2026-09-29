WITH cleaned AS (
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        CEIL(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock >= DATE '2023-07-21'
),
ranked AS (
    SELECT
        id,
        store_id,
        last_restock,
        quantity_whole
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) = 1
),
with_windows AS (
    SELECT
        id,
        quantity_whole,
        LAG(quantity_whole) OVER w_running AS prev_quantity_whole,
        LEAD(quantity_whole) OVER w_running AS next_quantity_whole,
        ROW_NUMBER() OVER w_running AS seq_no,
        SUM(quantity_whole) OVER (ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_whole
    FROM ranked
    WHERE MONTH(last_restock) IN (6, 9, 10)
    WINDOW w_running AS (PARTITION BY store_id ORDER BY quantity_whole, id)
),
enriched AS (
    SELECT
        *,
        LAG(prev_quantity_whole) OVER w_grp AS prev_prev_quantity_whole,
        LEAD(prev_quantity_whole) OVER w_grp AS next_prev_quantity_whole,
        LEAD(next_quantity_whole) OVER w_grp AS next_next_quantity_whole,
        ROW_NUMBER() OVER w_grp AS seq_no_2
    FROM with_windows
    WINDOW w_grp AS (ORDER BY quantity_whole, id)
),
enriched_inventory AS (
    SELECT
        id,
        quantity_whole,
        prev_quantity_whole,
        next_quantity_whole,
        seq_no,
        running_quantity_whole,
        next_prev_quantity_whole,
        next_next_quantity_whole,
        seq_no_2,
        MAX(quantity_whole) OVER (ORDER BY prev_prev_quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity_whole,
        AVG(quantity_whole) OVER (ORDER BY prev_prev_quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_quantity_whole,
        MIN(quantity_whole) OVER (ORDER BY prev_prev_quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_quantity_whole
    FROM enriched
),
filtered AS (
    SELECT
        id,
        quantity_whole,
        prev_quantity_whole,
        seq_no,
        running_quantity_whole,
        next_prev_quantity_whole,
        next_next_quantity_whole,
        seq_no_2,
        running_max_quantity_whole,
        running_avg_quantity_whole,
        running_min_quantity_whole,
        running_quantity_whole + next_quantity_whole AS running_quantity_whole_plus_next_quantity_whole,
        COALESCE(prev_quantity_whole, 0) AS prev_quantity_whole_filled
    FROM enriched_inventory
    WHERE running_max_quantity_whole <= 31
),
summary AS (
    SELECT
        seq_no,
        AVG(running_quantity_whole) AS avg_running_quantity_whole,
        PERCENTILE_APPROX(quantity_whole, 0.9) AS quantity_whole_p90,
        APPROX_PERCENTILE(prev_quantity_whole_filled, 0.5) AS prev_quantity_whole_filled_p50
    FROM filtered
    GROUP BY seq_no
    HAVING PERCENTILE_APPROX(quantity_whole, 0.9) < 1000000
)
SELECT
    seq_no,
    avg_running_quantity_whole,
    quantity_whole_p90
FROM summary
ORDER BY seq_no NULLS FIRST
