WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(quantity) AS quantity_whole,
        ROUND(quantity, 2) AS quantity_rounded
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-08-30')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        last_restock,
        LEAD(quantity, 1) OVER (PARTITION BY store_id ORDER BY quantity_rounded, id) AS next_quantity,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY quantity_rounded DESC, id) AS quantity_rounded_bucket,
        AVG(quantity_whole) OVER (PARTITION BY product_id, store_id ORDER BY quantity_whole, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_whole_avg_6_prior
    FROM valid
    WHERE store_id = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        last_restock,
        next_quantity,
        quantity_rounded_bucket,
        AVG(next_quantity) OVER w_recent AS running_avg_next_quantity,
        MIN(next_quantity) OVER w_recent AS running_min_next_quantity,
        COUNT(next_quantity) OVER w_recent AS running_count_next_quantity,
        MAX(next_quantity) OVER w_recent AS running_max_next_quantity
    FROM enriched
    WINDOW w_recent AS (ORDER BY next_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid_inventory AS (
    -- Basic cleansing and defaults
    SELECT *, running_max_next_quantity * next_quantity AS running_max_next_quantity_times_next_quantity
    FROM metrics
    WHERE last_restock >= TO_DATE('2023-05-28')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(running_max_next_quantity_times_next_quantity) OVER w_ordered AS running_max_next_quantity_times_next_quantity_avg_2_prior,
        ROW_NUMBER() OVER (ORDER BY running_max_next_quantity, id) AS seq_no
    FROM valid_inventory
    WINDOW w_ordered AS (ORDER BY quantity_rounded_bucket, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(running_max_next_quantity_times_next_quantity_avg_2_prior) IGNORE NULLS OVER (ORDER BY running_count_next_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_max_next_quantity_times_next_quantity_avg_2_prior,
        FIRST_VALUE(running_max_next_quantity_times_next_quantity) IGNORE NULLS OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_max_next_quantity_times_next_quantity
    FROM with_windows
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM scored
    EXCEPT
    SELECT id
    FROM inventory
    WHERE product_id IN (1, 7, 8, 10)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.id = k.id
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        seq_no,
        CASE WHEN running_max_next_quantity_times_next_quantity >= 83 THEN 'HIGH' ELSE 'LOW' END AS running_max_next_quantity_times_next_quantity_tier,
        ROUND(AVG(next_quantity), 2) AS avg_next_quantity,
        MAX(running_min_next_quantity) AS max_running_min_next_quantity,
        COUNT(*) AS cnt
    FROM verified
    GROUP BY GROUPING SETS ((seq_no, CASE WHEN running_max_next_quantity_times_next_quantity >= 83 THEN 'HIGH' ELSE 'LOW' END), (seq_no))
)
SELECT seq_no, max_running_min_next_quantity
FROM daily
ORDER BY max_running_min_next_quantity DESC, seq_no
