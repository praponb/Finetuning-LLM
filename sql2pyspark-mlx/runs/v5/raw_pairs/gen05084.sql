WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS next_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock, id) AS row_seq
    FROM inventory
    WHERE YEAR(last_restock) = 2024
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        next_quantity,
        row_seq
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC, id) AS rn
        FROM enriched
    ) x
    WHERE rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        next_quantity,
        row_seq,
        MAX(next_quantity) OVER w_recent AS running_max_next_quantity,
        COUNT(next_quantity) OVER w_recent AS running_count_next_quantity,
        AVG(next_quantity) OVER w_recent AS running_avg_next_quantity
    FROM ranked
    WINDOW w_recent AS (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(running_count_next_quantity) OVER (ORDER BY next_quantity, id) AS prev_running_count_next_quantity,
        LEAD(running_count_next_quantity) OVER (ORDER BY next_quantity, id) AS next_running_count_next_quantity,
        LEAD(next_quantity) OVER (ORDER BY next_quantity, id) AS next_next_quantity,
        LAG(next_quantity) OVER (ORDER BY next_quantity, id) AS prev_next_quantity,
        ROW_NUMBER() OVER (ORDER BY next_quantity, id) AS row_seq_2,
        SUM(running_max_next_quantity) OVER (ORDER BY running_count_next_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_max_next_quantity
    FROM with_windows
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(running_running_max_next_quantity, running_count_next_quantity) AS greatest_running_running_max_next_quantity_running_count_next_quantity
    FROM scored
    WHERE running_max_next_quantity >= 160
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_max_next_quantity >= 251 THEN 'LARGE' ELSE 'SMALL' END AS running_max_next_quantity_tier,
    CASE WHEN next_quantity >= 223 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_quantity_tier,
    APPROX_PERCENTILE(running_avg_next_quantity, ARRAY(0.25, 0.5, 0.75)) AS running_avg_next_quantity_quartiles,
    ROUND(AVG(running_max_next_quantity), 2) AS avg_running_max_next_quantity,
    RANK() OVER (PARTITION BY CASE WHEN running_max_next_quantity >= 251 THEN 'LARGE' ELSE 'SMALL' END ORDER BY ROUND(AVG(running_max_next_quantity), 2) DESC) AS avg_running_max_next_quantity_rank
FROM cleaned
GROUP BY CASE WHEN running_max_next_quantity >= 251 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN next_quantity >= 223 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY running_max_next_quantity_tier DESC
