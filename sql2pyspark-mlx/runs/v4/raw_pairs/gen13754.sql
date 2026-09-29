WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock DESC NULLS LAST, id) AS latest_rank
        FROM inventory
    ) t
    WHERE latest_rank = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        latest_rank,
        quantity + latest_rank AS quantity_plus_latest_rank
    FROM ranked
    WHERE store_id = 6
),
ranked_inventory AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY quantity_plus_latest_rank DESC NULLS LAST) = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM ranked_inventory
    WHERE last_restock >= DATE '2023-03-22'
      AND product_id IN (3, 4, 5, 7)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_plus_latest_rank DESC NULLS FIRST, id) AS latest_rank_2
        FROM valid
    ) t
    WHERE latest_rank_2 = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(quantity_filled) OVER (ORDER BY quantity_filled, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS quantity_filled_avg_3,
        ROW_NUMBER() OVER (ORDER BY quantity, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY quantity_filled DESC, id) AS event_seq,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity NULLS LAST) AS quantity_dense_rank,
        RANK() OVER (PARTITION BY store_id ORDER BY quantity_filled DESC NULLS FIRST) AS quantity_filled_rank
    FROM deduped
),
metrics_inventory AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (ORDER BY quantity_dense_rank DESC, id) AS quantity_dense_rank_bucket,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY latest_rank_2, id) AS seq_no_2,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY product_id), 4) AS seq_no_share,
        ROUND(quantity_filled_rank / SUM(quantity_filled_rank) OVER (PARTITION BY product_id, store_id), 4) AS quantity_filled_rank_share
    FROM metrics
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN quantity_plus_latest_rank >= 61 THEN 'LARGE' ELSE 'SMALL' END AS quantity_plus_latest_rank_tier,
        seq_no,
        DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
        ROUND(AVG(quantity_filled_avg_3), 2) AS avg_quantity_filled_avg_3,
        BIT_AND(quantity) AS quantity_bit_and,
        BIT_AND(quantity) AS quantity_bit_and_2
    FROM metrics_inventory
    GROUP BY GROUPING SETS ((CASE WHEN quantity_plus_latest_rank >= 61 THEN 'LARGE' ELSE 'SMALL' END, seq_no, DATE_TRUNC('QUARTER', last_restock)), (CASE WHEN quantity_plus_latest_rank >= 61 THEN 'LARGE' ELSE 'SMALL' END, seq_no))
)
SELECT
    quantity_plus_latest_rank_tier,
    seq_no,
    quarter_start,
    avg_quantity_filled_avg_3,
    quantity_bit_and,
    quantity_bit_and_2
FROM daily
