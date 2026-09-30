WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS quantity_avg_6,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS LAST) AS quantity_percent_rank,
        LEAD(quantity, 2) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS next_quantity,
        LEAD(quantity, 2) OVER (ORDER BY last_restock, id) AS next_quantity_2,
        NTILE(4) OVER (ORDER BY quantity DESC, id) AS quantity_bucket
    FROM inventory
    WHERE last_restock > '2024-09-13'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        last_restock,
        quantity_avg_6,
        quantity_percent_rank,
        next_quantity,
        quantity_bucket,
        SUM(quantity_percent_rank) OVER w_ordered AS running_sum_quantity_percent_rank,
        COUNT(quantity_percent_rank) OVER w_ordered AS running_count_quantity_percent_rank,
        MIN(quantity_percent_rank) OVER w_ordered AS running_min_quantity_percent_rank,
        MAX(quantity_percent_rank) OVER w_ordered AS running_max_quantity_percent_rank,
        ROW_NUMBER() OVER (ORDER BY quantity_percent_rank DESC, id) AS event_seq
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY product_id, store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        next_quantity,
        running_sum_quantity_percent_rank,
        running_min_quantity_percent_rank,
        event_seq,
        quantity_bucket * running_count_quantity_percent_rank AS quantity_bucket_times_running_count_quantity_percent_rank
    FROM scored
    WHERE last_restock > TO_DATE('2023-06-24')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(next_quantity, running_sum_quantity_percent_rank) AS greatest_next_quantity_running_sum_quantity_percent_rank
    FROM cleaned
    WHERE event_seq BETWEEN 4 AND 6
)
-- Grouped metrics
SELECT
    CASE WHEN running_sum_quantity_percent_rank >= 4.5 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_sum_quantity_percent_rank_tier,
    APPROX_PERCENTILE(running_sum_quantity_percent_rank, 0.95) AS running_sum_quantity_percent_rank_p95,
    APPROX_PERCENTILE(greatest_next_quantity_running_sum_quantity_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS greatest_next_quantity_running_sum_quantity_percent_rank_quartiles,
    MAX(event_seq) AS max_event_seq
FROM valid
GROUP BY CASE WHEN running_sum_quantity_percent_rank >= 4.5 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING MAX(event_seq) > 0
ORDER BY running_sum_quantity_percent_rank_p95 NULLS LAST;
