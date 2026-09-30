WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        quantity,
        LEAD(quantity) OVER (PARTITION BY product_id, store_id ORDER BY last_restock, id) AS next_quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock, id) AS seq_no,
        LEAD(quantity, 2) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS next_quantity_2
    FROM inventory
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        quantity,
        next_quantity,
        seq_no,
        MAX(seq_no) OVER (PARTITION BY store_id ORDER BY next_quantity_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_seq_no,
        SUM(seq_no) OVER (PARTITION BY store_id ORDER BY next_quantity_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_seq_no,
        MIN(seq_no) OVER (PARTITION BY store_id ORDER BY next_quantity_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_seq_no,
        SUM(seq_no) OVER (ORDER BY next_quantity_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no
    FROM enriched
    WHERE quantity IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY running_seq_no DESC, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY running_min_seq_no DESC, id) AS row_num
    FROM ranked
    QUALIFY row_num = 1
),
enriched_inventory AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.next_quantity,
        src.running_sum_seq_no,
        src.row_num,
        p.opened_date,
        p.sqft
    FROM latest src
    LEFT JOIN stores p
        ON src.store_id = p.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        sqft,
        LAST_VALUE(next_quantity) IGNORE NULLS OVER w_part AS last_known_next_quantity,
        COUNT(next_quantity) OVER w_part_2 AS store_id_count_next_quantity,
        SUM(sqft) OVER w_grp AS running_sqft,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY running_sum_seq_no NULLS FIRST) AS running_sum_seq_no_percent_rank
    FROM enriched_inventory
    WINDOW w_part AS (PARTITION BY store_id ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part_2 AS (PARTITION BY store_id), w_grp AS (PARTITION BY store_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    store_id_count_next_quantity,
    running_sum_seq_no_percent_rank
FROM scored
ORDER BY store_id_count_next_quantity
