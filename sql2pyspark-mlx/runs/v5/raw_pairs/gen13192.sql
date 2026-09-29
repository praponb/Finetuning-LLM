WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DATE_TRUNC('week', last_restock) AS last_restock_period,
        ADD_MONTHS(last_restock, 3) AS last_restock_plus_months
    FROM inventory
    WHERE last_restock >= DATE '2023-05-28'
      AND store_id IN (1, 3, 5, 6)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        last_restock_plus_months,
        MIN(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_quantity,
        COUNT(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_quantity,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_quantity,
        SUM(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS seq_no
    FROM filtered
    WHERE store_id IN (1, 2, 6)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        last_restock_plus_months,
        last3_avg_quantity,
        seq_no
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY seq_no DESC NULLS LAST, id) AS dedup_rank
        FROM enriched
    ) t
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last_restock_plus_months,
        FIRST_VALUE(last3_avg_quantity, TRUE) OVER (ORDER BY last_restock_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last3_avg_quantity,
        ROW_NUMBER() OVER (ORDER BY last3_avg_quantity, id) AS seq_no_2,
        LAST_VALUE(last3_avg_quantity, TRUE) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last3_avg_quantity
    FROM ranked
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(first_known_last3_avg_quantity) OVER (ORDER BY seq_no_2, id) AS prev_first_known_last3_avg_quantity,
        LAG(last_known_last3_avg_quantity) OVER (ORDER BY seq_no_2, id) AS prev_last_known_last3_avg_quantity,
        LEAD(last_known_last3_avg_quantity) OVER (ORDER BY seq_no_2, id) AS next_last_known_last3_avg_quantity,
        ROW_NUMBER() OVER (ORDER BY seq_no_2, id) AS event_seq
    FROM with_windows
)
SELECT
    id,
    last_restock_plus_months,
    first_known_last3_avg_quantity,
    seq_no_2,
    last_known_last3_avg_quantity,
    prev_first_known_last3_avg_quantity,
    prev_last_known_last3_avg_quantity,
    next_last_known_last3_avg_quantity,
    event_seq
FROM metrics
ORDER BY first_known_last3_avg_quantity NULLS LAST, seq_no_2 DESC
