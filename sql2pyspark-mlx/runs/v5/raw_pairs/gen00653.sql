WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        UPPER(store_name) AS store_name_upper,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM stores
    WHERE region IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        sqft,
        store_name_upper,
        MIN(sqft) OVER w_recent AS running_min_sqft,
        MAX(sqft) OVER w_recent AS running_max_sqft,
        AVG(sqft) OVER w_recent AS running_avg_sqft
    FROM valid
    WINDOW w_recent AS (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name_upper,
        s.running_avg_sqft,
        latest.last_restock,
        latest.inventory_quantity
    FROM enriched s
    JOIN LATERAL (
            SELECT c.last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name_upper,
        last_restock,
        NTILE(10) OVER w_hist AS inventory_quantity_bucket,
        ROW_NUMBER() OVER (ORDER BY last_restock DESC, id) AS seq_no,
        ROW_NUMBER() OVER w_part AS row_seq,
        NTILE(3) OVER w_hist AS inventory_quantity_bucket_2
    FROM with_last
    WINDOW w_hist AS (ORDER BY inventory_quantity, id), w_part AS (ORDER BY running_avg_sqft DESC, id)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        inventory_quantity_bucket,
        DENSE_RANK() OVER (ORDER BY inventory_quantity_bucket_2 NULLS LAST) AS inventory_quantity_bucket_2_dense_rank,
        SUM(seq_no) OVER w_recent AS cumulative_seq_no,
        AVG(row_seq) OVER (ORDER BY seq_no, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS row_seq_avg_5,
        SUM(inventory_quantity_bucket) OVER (ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_inventory_quantity_bucket
    FROM scored
    WINDOW w_recent AS (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last_stores AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.last_restock
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    inventory_quantity_bucket_2_dense_rank,
    cumulative_seq_no,
    row_seq_avg_5,
    running_inventory_quantity_bucket
FROM with_last_stores
