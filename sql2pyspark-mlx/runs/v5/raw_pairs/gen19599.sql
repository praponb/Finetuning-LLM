WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_TRUNC('YEAR', last_restock) AS last_restock_period
    FROM inventory
    WHERE last_restock > DATE '2023-09-06'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(quantity) OVER w_running AS running_sum_quantity,
        MAX(quantity) OVER w_running AS running_max_quantity,
        COUNT(quantity) OVER w_running AS running_count_quantity,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY last_restock_period, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS quantity_avg_5
    FROM valid
    WINDOW w_running AS (PARTITION BY product_id, store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(running_count_quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_running_count_quantity,
        AVG(running_count_quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_running_count_quantity,
        SUM(running_count_quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_count_quantity,
        COUNT(running_count_quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_count_quantity,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY running_max_quantity, id) AS running_max_quantity_bucket
    FROM metrics
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        last_restock_period,
        running_avg_running_count_quantity,
        running_sum_running_count_quantity,
        COUNT(running_count_running_count_quantity) OVER w_part AS running_count_running_count_running_count_quantity,
        MAX(running_count_running_count_quantity) OVER w_part AS running_max_running_count_running_count_quantity,
        AVG(running_count_running_count_quantity) OVER w_part AS running_avg_running_count_running_count_quantity
    FROM scored
    WINDOW w_part AS (PARTITION BY product_id ORDER BY running_max_quantity_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
joined AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.last_restock,
        a.running_avg_running_count_quantity,
        a.running_sum_running_count_quantity,
        a.running_count_running_count_running_count_quantity,
        p.sqft,
        p.store_name
    FROM with_windows a
    JOIN stores p
        ON a.store_id = p.id
),
scored_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        last_restock,
        running_avg_running_count_quantity,
        running_sum_running_count_quantity,
        sqft,
        store_name,
        AVG(sqft) OVER w_grp AS sqft_avg_6,
        AVG(running_avg_running_count_quantity) OVER (PARTITION BY store_id ORDER BY running_sum_running_count_quantity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_avg_running_count_quantity_avg_5_prior,
        SUM(sqft) OVER (PARTITION BY store_id ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft
    FROM joined
    WHERE store_id IN (1, 4)
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY running_count_running_count_running_count_quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
scored_inventory_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        last_restock,
        running_avg_running_count_quantity,
        running_avg_running_count_quantity_avg_5_prior,
        running_sqft,
        AVG(sqft_avg_6) OVER (ORDER BY running_avg_running_count_quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sqft_avg_6_avg_3,
        AVG(running_sqft) OVER (PARTITION BY store_id ORDER BY running_avg_running_count_quantity_avg_5_prior, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS running_sqft_avg_5,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY running_avg_running_count_quantity_avg_5_prior DESC, id) AS running_avg_running_count_quantity_avg_5_prior_bucket
    FROM scored_inventory
)
-- Running and moving aggregates
SELECT
    id,
    last_restock,
    running_avg_running_count_quantity,
    running_avg_running_count_quantity_avg_5_prior,
    running_sqft_avg_5,
    running_avg_running_count_quantity_avg_5_prior_bucket,
    AVG(running_avg_running_count_quantity_avg_5_prior_bucket) OVER w_running AS running_avg_running_count_quantity_avg_5_prior_bucket_avg_3_prior,
    SUM(running_sqft_avg_5) OVER w_part AS cumulative_running_sqft_avg_5,
    MIN(running_avg_running_count_quantity_avg_5_prior_bucket) OVER (PARTITION BY store_id) AS store_id_min_running_avg_running_count_quantity_avg_5_prior_bucket,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sqft_avg_6_avg_3, id) AS seq_no,
    LAST_VALUE(running_sqft_avg_5) IGNORE NULLS OVER (ORDER BY running_avg_running_count_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_sqft_avg_5
FROM scored_inventory_2
WINDOW w_running AS (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), w_part AS (PARTITION BY store_id ORDER BY running_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
