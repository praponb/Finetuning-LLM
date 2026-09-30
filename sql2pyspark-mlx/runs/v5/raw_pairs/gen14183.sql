WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        CASE WHEN city = 'New York' THEN 1 ELSE 0 END AS is_new_york,
        DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_date_label
    FROM stores
    WHERE sqft BETWEEN 8456 AND 11082
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        opened_date_label,
        MAX(sqft) OVER w_ordered AS running_max_sqft,
        MIN(sqft) OVER w_ordered AS running_min_sqft,
        SUM(sqft) OVER w_ordered AS running_sum_sqft,
        COUNT(sqft) OVER w_ordered AS running_count_sqft,
        AVG(sqft) OVER (ORDER BY is_new_york, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS sqft_avg_3_prior
    FROM filtered
    WHERE region NOT IN ('South', 'East', 'Central')
    WINDOW w_ordered AS (PARTITION BY city, region ORDER BY is_new_york, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY running_count_sqft DESC, id) AS dedup_rank
    FROM metrics
    QUALIFY dedup_rank = 1
),
filtered_stores AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        opened_date_label,
        running_max_sqft,
        running_min_sqft,
        running_sum_sqft,
        running_count_sqft,
        sqft_avg_3_prior,
        CAST(running_max_sqft AS INT) AS running_max_sqft_int,
        dedup_rank - running_max_sqft AS dedup_rank_minus_running_max_sqft
    FROM latest
    WHERE running_min_sqft <= 15100
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.inventory_last_restock
    FROM filtered_stores s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        opened_date_label,
        running_max_sqft,
        running_min_sqft,
        running_sum_sqft,
        running_count_sqft,
        sqft_avg_3_prior,
        running_max_sqft_int,
        dedup_rank_minus_running_max_sqft,
        inventory_last_restock,
        FLOOR(running_sum_sqft) AS running_sum_sqft_whole,
        CASE WHEN dedup_rank_minus_running_max_sqft >= 62 THEN 'High' WHEN dedup_rank_minus_running_max_sqft >= 30 THEN 'Medium' ELSE 'Low' END AS dedup_rank_minus_running_max_sqft_tier
    FROM with_latest
    WHERE MONTH(inventory_last_restock) IN (1, 3, 6)
),
report AS (
    SELECT
        id,
        opened_date_label,
        running_max_sqft,
        running_min_sqft,
        sqft_avg_3_prior,
        dedup_rank_minus_running_max_sqft,
        inventory_last_restock,
        running_sum_sqft_whole,
        dedup_rank_minus_running_max_sqft_tier
    FROM valid
    ORDER BY running_sum_sqft_whole DESC
)
SELECT
    id,
    sqft_avg_3_prior,
    dedup_rank_minus_running_max_sqft,
    running_sum_sqft_whole,
    dedup_rank_minus_running_max_sqft_tier
FROM report
ORDER BY id NULLS FIRST, running_sum_sqft_whole DESC;
