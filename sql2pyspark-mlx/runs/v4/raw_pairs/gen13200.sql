WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC NULLS LAST, id) AS dedup_rank
        FROM order_items
    ) ranked
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY unit_price DESC) AS unit_price_dense_rank,
        LAST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        NTILE(3) OVER (ORDER BY unit_price DESC, id) AS unit_price_bucket
    FROM deduped
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        dedup_rank,
        last_known_quantity,
        unit_price_bucket,
        ROUND(quantity, 0) AS quantity_rounded,
        dedup_rank * last_known_quantity AS dedup_rank_times_last_known_quantity
    FROM with_windows
    WHERE unit_price_bucket <> 7
)
SELECT
    id,
    order_id,
    product_id,
    dedup_rank,
    last_known_quantity,
    quantity_rounded,
    dedup_rank_times_last_known_quantity
FROM filtered;
