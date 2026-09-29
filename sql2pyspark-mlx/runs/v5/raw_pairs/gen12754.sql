WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id, store_id ORDER BY last_restock NULLS FIRST) AS dedup_rank
        FROM inventory
    ) t
    WHERE dedup_rank = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        dedup_rank,
        quantity - dedup_rank AS quantity_minus_dedup_rank
    FROM deduped
    WHERE last_restock BETWEEN '2023-09-01' AND '2023-10-01'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_minus_dedup_rank,
        LAG(quantity_minus_dedup_rank) OVER (PARTITION BY product_id ORDER BY quantity_minus_dedup_rank, id) AS prev_quantity_minus_dedup_rank,
        LEAD(quantity) OVER (PARTITION BY product_id ORDER BY quantity_minus_dedup_rank, id) AS next_quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_minus_dedup_rank, id) AS event_seq,
        LEAD(quantity_minus_dedup_rank, 1) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS next_quantity_minus_dedup_rank
    FROM cleaned
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        quantity_minus_dedup_rank,
        prev_quantity_minus_dedup_rank,
        event_seq,
        next_quantity_minus_dedup_rank,
        SUM(quantity) OVER (ORDER BY next_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        AVG(quantity_minus_dedup_rank) OVER (PARTITION BY product_id, store_id ORDER BY prev_quantity_minus_dedup_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS quantity_minus_dedup_rank_avg_3,
        LEAD(quantity_minus_dedup_rank, 2) OVER (PARTITION BY product_id ORDER BY next_quantity, id) AS next_quantity_minus_dedup_rank_2,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY quantity_minus_dedup_rank DESC, id) AS quantity_minus_dedup_rank_bucket,
        ROW_NUMBER() OVER (ORDER BY last_restock, id) AS event_seq_2
    FROM scored
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        quantity_minus_dedup_rank,
        prev_quantity_minus_dedup_rank,
        next_quantity_minus_dedup_rank,
        cumulative_quantity,
        quantity_minus_dedup_rank_avg_3,
        next_quantity_minus_dedup_rank_2,
        quantity_minus_dedup_rank_bucket,
        event_seq_2 + next_quantity_minus_dedup_rank_2 AS event_seq_2_plus_next_quantity_minus_dedup_rank_2
    FROM metrics
    WHERE store_id = 1
),
base_inventory AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity_minus_dedup_rank,
        prev_quantity_minus_dedup_rank,
        next_quantity_minus_dedup_rank,
        cumulative_quantity,
        quantity_minus_dedup_rank_bucket,
        ROUND(next_quantity_minus_dedup_rank_2 * 1.1, 2) AS adjusted_next_quantity_minus_dedup_rank_2
    FROM base
    WHERE next_quantity_minus_dedup_rank_2 IS NOT NULL
)
-- Ranking and gap features
SELECT
    id,
    product_id,
    cumulative_quantity,
    quantity_minus_dedup_rank_bucket,
    FIRST_VALUE(prev_quantity_minus_dedup_rank) IGNORE NULLS OVER (ORDER BY quantity_minus_dedup_rank_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_quantity_minus_dedup_rank,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY next_quantity_minus_dedup_rank DESC, id) AS seq_no,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY next_quantity_minus_dedup_rank, id) AS row_seq
FROM base_inventory;
