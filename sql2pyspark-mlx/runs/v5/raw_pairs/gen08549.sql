WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        COALESCE(quantity, 0) AS quantity_filled,
        ROUND(quantity * 1.15, 2) AS quantity_scaled
    FROM inventory
    WHERE product_id IN (5, 10)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity_filled, id) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY row_num, id) AS rn
        FROM deduped
    ) t
    WHERE rn = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        quantity_scaled,
        row_num,
        rn,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS quantity_avg_5_prior,
        LAG(quantity_scaled, 1) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS prev_quantity_scaled,
        NTILE(4) OVER w_hist AS quantity_filled_bucket
    FROM latest
    WINDOW w_hist AS (ORDER BY quantity_filled DESC, id)
),
enriched AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.quantity_scaled,
        src.rn,
        src.quantity_filled_bucket,
        s.sqft
    FROM scored src
    INNER JOIN stores s
        ON src.store_id = s.id
),
deduped_inventory AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY quantity_scaled DESC NULLS LAST) AS rn_2
    FROM enriched
    QUALIFY rn_2 = 1
),
latest_inventory AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity_scaled, id) AS dedup_rank
    FROM deduped_inventory
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    quantity_scaled,
    rn,
    quantity_filled_bucket,
    rn_2,
    dedup_rank
FROM latest_inventory
ORDER BY id, rn_2;
