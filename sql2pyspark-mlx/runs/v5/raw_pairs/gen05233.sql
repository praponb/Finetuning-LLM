WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock > DATE '2023-05-29'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_whole, id) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(quantity_whole) OVER w_part AS product_id_min_quantity_whole,
        AVG(quantity_whole) OVER w_recent AS quantity_whole_avg_2,
        LEAD(quantity, 1, 0) OVER (PARTITION BY store_id ORDER BY quantity, id) AS next_quantity
    FROM latest
    WINDOW w_part AS (PARTITION BY product_id), w_recent AS (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
latest_inventory AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY next_quantity, id) AS row_num
        FROM metrics
    ) x
    WHERE row_num = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER w_recent AS quantity_whole_avg_2_bucket,
        SUM(next_quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_quantity,
        LAST_VALUE(quantity_whole_avg_2) IGNORE NULLS OVER (PARTITION BY product_id, store_id ORDER BY quantity_whole_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_whole_avg_2
    FROM latest_inventory
    WHERE store_id = 4
    WINDOW w_recent AS (PARTITION BY store_id ORDER BY quantity_whole_avg_2 DESC, id)
)
-- Running and moving aggregates
SELECT
    id,
    store_id,
    quantity,
    last_restock,
    quantity_whole,
    product_id_min_quantity_whole,
    quantity_whole_avg_2,
    next_quantity,
    quantity_whole_avg_2_bucket,
    cumulative_next_quantity,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY cumulative_next_quantity, id) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity DESC, id) AS seq_no,
    LAST_VALUE(last_known_quantity_whole_avg_2, TRUE) OVER (PARTITION BY store_id ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_known_quantity_whole_avg_2,
    STDDEV_SAMP(quantity_whole_avg_2) OVER (PARTITION BY store_id) AS store_id_stddev_samp_quantity_whole_avg_2,
    FIRST_VALUE(quantity_whole_avg_2) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_whole_avg_2
FROM enriched;
