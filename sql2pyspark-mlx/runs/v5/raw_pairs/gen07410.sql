WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE product_id = 10
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, quantity - days_since_last_restock AS quantity_minus_days_since_last_restock
    FROM cleaned
    WHERE last_restock >= TO_DATE('2023-08-12')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        quantity,
        days_since_last_restock,
        quantity_minus_days_since_last_restock,
        NTILE(3) OVER (ORDER BY days_since_last_restock DESC, id) AS days_since_last_restock_bucket,
        ROW_NUMBER() OVER (ORDER BY last_restock DESC, id) AS seq_no,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY quantity, id) AS quantity_bucket
    FROM valid
),
enriched_inventory AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(quantity_bucket) OVER (PARTITION BY store_id) AS store_id_max_quantity_bucket,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY days_since_last_restock_bucket DESC NULLS LAST) AS days_since_last_restock_bucket_percent_rank,
        SUM(days_since_last_restock_bucket) OVER (PARTITION BY store_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_last_restock_bucket
    FROM enriched
    WHERE store_id IN (4, 6)
),
cleaned_inventory AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        days_since_last_restock * days_since_last_restock_bucket_percent_rank AS days_since_last_restock_times_days_since_last_restock_bucket_percent_rank,
        days_since_last_restock * running_days_since_last_restock_bucket AS days_since_last_restock_times_running_days_since_last_restock_bucket
    FROM enriched_inventory
    WHERE days_since_last_restock >= 43
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        days_since_last_restock,
        seq_no,
        quantity_bucket,
        days_since_last_restock_bucket_percent_rank,
        days_since_last_restock_times_days_since_last_restock_bucket_percent_rank,
        DENSE_RANK() OVER (ORDER BY days_since_last_restock DESC) AS days_since_last_restock_dense_rank,
        NTILE(10) OVER (PARTITION BY store_id ORDER BY seq_no, id) AS seq_no_bucket,
        PERCENT_RANK() OVER w_ordered AS days_since_last_restock_bucket_percent_rank_percent_rank
    FROM cleaned_inventory
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY days_since_last_restock_bucket_percent_rank)
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.*,
        d.region,
        d.opened_date,
        d.city
    FROM with_windows src
    LEFT JOIN stores d
        ON src.store_id = d.id
)
-- Aggregate with conditional metrics
SELECT
    region,
    COUNT(*) FILTER (WHERE region = 'Central') AS central_rows,
    COUNT(DISTINCT store_id) AS distinct_store_ids
FROM joined
GROUP BY 1
