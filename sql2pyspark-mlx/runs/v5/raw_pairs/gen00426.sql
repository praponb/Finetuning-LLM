WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        quantity,
        DATE_ADD(last_restock, 21) AS last_restock_plus_21d
    FROM inventory
    WHERE last_restock > DATE '2023-04-14'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        last_restock_plus_21d,
        LAG(quantity, 1) OVER (ORDER BY quantity, id) AS prev_quantity,
        DENSE_RANK() OVER w_part AS quantity_dense_rank,
        NTILE(10) OVER w_part_2 AS quantity_bucket,
        LAST_VALUE(quantity, TRUE) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity
    FROM valid
    WINDOW w_part AS (ORDER BY quantity DESC NULLS LAST), w_part_2 AS (ORDER BY quantity DESC, id)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity_dense_rank,
        last_known_quantity,
        FIRST_VALUE(quantity_dense_rank, TRUE) OVER (ORDER BY last_known_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_dense_rank,
        DENSE_RANK() OVER (ORDER BY last_known_quantity DESC NULLS LAST) AS last_known_quantity_dense_rank,
        PERCENT_RANK() OVER (ORDER BY last_known_quantity DESC NULLS FIRST) AS last_known_quantity_percent_rank
    FROM enriched
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        last_known_quantity,
        last_known_quantity_dense_rank,
        LEAD(last_known_quantity_dense_rank) OVER w_recent AS next_last_known_quantity_dense_rank,
        LAG(last_known_quantity_dense_rank) OVER w_recent AS prev_last_known_quantity_dense_rank,
        LEAD(first_known_quantity_dense_rank) OVER w_recent AS next_first_known_quantity_dense_rank,
        ROW_NUMBER() OVER w_recent AS row_seq,
        RANK() OVER (ORDER BY last_known_quantity_dense_rank DESC NULLS FIRST) AS last_known_quantity_dense_rank_rank
    FROM scored
    WINDOW w_recent AS (ORDER BY quantity_dense_rank, id)
),
with_windows_inventory AS (
    -- Ranking and gap features
    SELECT
        id,
        last_known_quantity_dense_rank,
        next_first_known_quantity_dense_rank,
        last_known_quantity_dense_rank_rank,
        SUM(prev_last_known_quantity_dense_rank) OVER (ORDER BY last_known_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_prev_last_known_quantity_dense_rank,
        COUNT(prev_last_known_quantity_dense_rank) OVER (ORDER BY last_known_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_prev_last_known_quantity_dense_rank,
        AVG(prev_last_known_quantity_dense_rank) OVER (ORDER BY last_known_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_prev_last_known_quantity_dense_rank,
        MAX(prev_last_known_quantity_dense_rank) OVER (ORDER BY last_known_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_prev_last_known_quantity_dense_rank
    FROM with_windows
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        last_known_quantity_dense_rank,
        next_first_known_quantity_dense_rank,
        last_known_quantity_dense_rank_rank,
        running_sum_prev_last_known_quantity_dense_rank,
        running_count_prev_last_known_quantity_dense_rank,
        CASE WHEN last_known_quantity_dense_rank_rank >= 7 THEN 'High' WHEN last_known_quantity_dense_rank_rank >= 4 THEN 'Medium' ELSE 'Low' END AS last_known_quantity_dense_rank_rank_band,
        ROUND(next_first_known_quantity_dense_rank * 100, 2) AS next_first_known_quantity_dense_rank_adjusted
    FROM with_windows_inventory
    WHERE running_avg_prev_last_known_quantity_dense_rank < 7.9
),
filtered_inventory AS (
    -- Keep recent, valid rows
    SELECT *, last_known_quantity_dense_rank_rank + last_known_quantity_dense_rank AS last_known_quantity_dense_rank_rank_plus_last_known_quantity_dense_rank
    FROM filtered
    WHERE running_sum_prev_last_known_quantity_dense_rank < 16
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_count_prev_last_known_quantity_dense_rank >= 14 THEN 'LARGE' ELSE 'SMALL' END AS running_count_prev_last_known_quantity_dense_rank_tier,
    CASE WHEN last_known_quantity_dense_rank_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS last_known_quantity_dense_rank_rank_tier,
    CASE WHEN last_known_quantity_dense_rank_rank_plus_last_known_quantity_dense_rank >= 74 THEN 'LARGE' ELSE 'SMALL' END AS last_known_quantity_dense_rank_rank_plus_last_known_quantity_dense_rank_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM filtered_inventory
GROUP BY CUBE (CASE WHEN running_count_prev_last_known_quantity_dense_rank >= 14 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last_known_quantity_dense_rank_rank >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN last_known_quantity_dense_rank_rank_plus_last_known_quantity_dense_rank >= 74 THEN 'LARGE' ELSE 'SMALL' END);
