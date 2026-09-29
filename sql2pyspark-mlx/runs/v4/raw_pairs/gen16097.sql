WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity, id) AS row_seq,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS quantity_percent_rank,
        RANK() OVER (PARTITION BY store_id ORDER BY quantity NULLS FIRST) AS quantity_rank
    FROM inventory
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        row_seq,
        quantity_percent_rank,
        quantity_rank,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY quantity_rank, id) AS seq_no,
        SUM(quantity_rank) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_rank
    FROM metrics
),
with_windows_inventory AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(quantity_percent_rank) OVER (PARTITION BY store_id, product_id ORDER BY cumulative_quantity_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_percent_rank,
        LAST_VALUE(seq_no) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY quantity_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no,
        AVG(seq_no) OVER (PARTITION BY store_id ORDER BY cumulative_quantity_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS seq_no_avg_3_prior
    FROM with_windows
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        d.category,
        d.product_name
    FROM with_windows_inventory a
    LEFT JOIN products d
        ON a.product_id = d.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category, store_id ORDER BY quantity NULLS LAST, id) AS rank_in_group
    FROM with_dims
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    product_id,
    last_restock,
    row_seq,
    quantity_percent_rank,
    quantity_rank,
    seq_no,
    cumulative_quantity_rank,
    running_quantity_percent_rank
FROM deduped
ORDER BY product_id;
