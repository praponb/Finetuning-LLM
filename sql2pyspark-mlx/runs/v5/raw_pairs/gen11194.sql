WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price, id) AS dedup_rank
    FROM order_items
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(unit_price) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_unit_price,
        MAX(unit_price) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_unit_price,
        COUNT(unit_price) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_unit_price,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS row_seq
    FROM deduped
    WHERE order_id = 26
),
deduped_order_items AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS dedup_rank_2
        FROM enriched
    ) ranked
    WHERE dedup_rank_2 = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY running_sum_unit_price NULLS FIRST, id) AS rn
        FROM deduped_order_items
    ) x
    WHERE rn = 1
),
report AS (
    SELECT
        id,
        order_id,
        quantity,
        dedup_rank,
        running_max_unit_price,
        running_count_unit_price,
        row_seq,
        dedup_rank_2,
        rn
    FROM latest
    ORDER BY rn NULLS LAST
)
SELECT
    id,
    order_id,
    quantity,
    dedup_rank,
    running_max_unit_price,
    running_count_unit_price,
    row_seq,
    rn
FROM report
ORDER BY id DESC;
