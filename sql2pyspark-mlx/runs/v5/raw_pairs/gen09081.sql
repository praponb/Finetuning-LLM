WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY last_restock) AS row_num
    FROM inventory
    QUALIFY row_num = 1
),
latest_inventory AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock DESC, id) AS dedup_rank
        FROM latest
    ) t
    WHERE dedup_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY quantity DESC, id) AS event_seq_2,
        LAST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY product_id, store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        SUM(quantity) OVER (PARTITION BY product_id) AS product_id_sum_quantity,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior
    FROM latest_inventory
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, event_seq + dedup_rank AS event_seq_plus_dedup_rank
    FROM scored
    WHERE quantity_avg_6_prior IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        row_num,
        dedup_rank,
        event_seq,
        last_known_quantity,
        product_id_sum_quantity,
        quantity_avg_6_prior,
        event_seq_plus_dedup_rank,
        last_known_quantity + event_seq_2 AS last_known_quantity_plus_event_seq_2
    FROM cleaned
    WHERE last_restock >= TO_DATE('2023-03-17')
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', last_restock) AS year_start,
    product_id,
    CASE WHEN quantity >= 255 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    BIT_AND(quantity) AS quantity_bit_and,
    ROUND(AVG(quantity_avg_6_prior), 2) AS avg_quantity_avg_6_prior,
    MIN(event_seq_plus_dedup_rank) AS min_event_seq_plus_dedup_rank,
    GROUPING_ID() AS grouping_id
FROM filtered
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', last_restock), product_id, CASE WHEN quantity >= 255 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('YEAR', last_restock), product_id), (DATE_TRUNC('YEAR', last_restock)), ())
ORDER BY avg_quantity_avg_6_prior DESC NULLS FIRST
