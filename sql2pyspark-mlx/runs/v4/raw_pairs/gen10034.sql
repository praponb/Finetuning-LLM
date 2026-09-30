WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        CEIL(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock > DATE '2023-08-21'
      AND quantity >= 231
),
metrics AS (
    SELECT
        *,
        LAST_VALUE(quantity_whole) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY quantity_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_whole,
        ROW_NUMBER() OVER (ORDER BY quantity, id) AS event_seq
    FROM cleaned
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity_whole, id) AS rn
    FROM metrics
    QUALIFY rn = 1
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS latest_rank
        FROM ranked
    ) ranked
    WHERE latest_rank = 1
),
enriched AS (
    SELECT
        src.*,
        p.product_name,
        p.launch_date,
        p.category
    FROM deduped src
    LEFT JOIN products p
        ON src.product_id = p.id
)
SELECT *
FROM (
    SELECT
        store_id,
        product_id,
        category,
        last_known_quantity_whole
    FROM enriched
) src
PIVOT (
    SUM(last_known_quantity_whole) AS value_sum,
    COUNT(1) AS item_count
    FOR category IN ('Grocery' AS grocery, 'Books' AS books, 'Fashion' AS fashion)
);
