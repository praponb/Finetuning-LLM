WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        quantity,
        ROW_NUMBER() OVER (ORDER BY last_restock, id) AS event_seq,
        LAST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity NULLS FIRST) AS quantity_dense_rank,
        NTILE(5) OVER (ORDER BY quantity, id) AS quantity_bucket
    FROM inventory
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, last_known_quantity * cumulative_quantity AS last_known_quantity_times_cumulative_quantity
    FROM with_windows
    WHERE quantity_bucket IS NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        quantity,
        event_seq,
        last_known_quantity,
        cumulative_quantity,
        quantity_dense_rank,
        quantity_bucket,
        last_known_quantity_times_cumulative_quantity,
        CEIL(last_known_quantity_times_cumulative_quantity) AS last_known_quantity_times_cumulative_quantity_whole,
        event_seq / NULLIF(cumulative_quantity, 0) AS event_seq_per_cumulative_quantity
    FROM valid
    WHERE product_id IN (1, 4, 5)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        event_seq,
        last_known_quantity,
        quantity_dense_rank,
        quantity_bucket,
        last_known_quantity_times_cumulative_quantity,
        event_seq_per_cumulative_quantity,
        quantity_bucket / NULLIF(last_known_quantity_times_cumulative_quantity, 0) AS quantity_bucket_per_last_known_quantity_times_cumulative_quantity,
        CEIL(event_seq) AS event_seq_whole
    FROM cleaned
    WHERE cumulative_quantity > 1106
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN last_known_quantity_times_cumulative_quantity >= 67 THEN 'LARGE' ELSE 'SMALL' END AS last_known_quantity_times_cumulative_quantity_tier,
        CASE WHEN quantity_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS quantity_bucket_tier,
        CASE WHEN event_seq_whole >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_whole_tier,
        MIN(last_known_quantity) AS min_last_known_quantity,
        MIN(quantity_bucket_per_last_known_quantity_times_cumulative_quantity) AS min_quantity_bucket_per_last_known_quantity_times_cumulative_quantity,
        PERCENTILE_APPROX(event_seq, 0.9) AS event_seq_p90
    FROM base
    GROUP BY ROLLUP (CASE WHEN last_known_quantity_times_cumulative_quantity >= 67 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN quantity_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN event_seq_whole >= 74 THEN 'PREMIUM' ELSE 'STANDARD' END)
)
SELECT
    quantity_bucket_tier,
    event_seq_whole_tier,
    min_last_known_quantity,
    min_quantity_bucket_per_last_known_quantity_times_cumulative_quantity,
    event_seq_p90
FROM totals
ORDER BY min_last_known_quantity;
