WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        ROUND(quantity * 0.85, 2) quantity_scaled,
        CASE WHEN quantity >= 8 THEN 'High' WHEN quantity >= 3 THEN 'Medium' ELSE 'Low' END quantity_tier
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT src.*, d.order_date
    FROM base src
    INNER JOIN orders d
        ON src.order_id = d.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_grp seq_no,
        FIRST_VALUE(quantity_scaled, TRUE) OVER (PARTITION BY order_id ORDER BY quantity_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_quantity_scaled
    FROM with_dims
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY order_date DESC, id)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity NULLS LAST, id) = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        order_date,
        ROUND(seq_no, 1) seq_no_rounded,
        quantity * seq_no quantity_times_seq_no
    FROM deduped
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Attach products attributes
    SELECT a.*, b.launch_date
    FROM filtered a
    LEFT JOIN products b
        ON a.product_id = b.id
)
SELECT
    id,
    order_id,
    order_date,
    quantity_times_seq_no,
    launch_date
FROM enriched
ORDER BY order_id DESC NULLS FIRST;
