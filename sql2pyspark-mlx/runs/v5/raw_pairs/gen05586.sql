WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        GREATEST(quantity, unit_price) AS greatest_quantity_unit_price
    FROM order_items
    WHERE product_id IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) = 1
),
joined AS (
    -- Attach orders attributes
    SELECT e.*, b.channel
    FROM deduped e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        unit_price,
        LAG(quantity, 1, 0) OVER (PARTITION BY product_id, channel ORDER BY quantity, id) AS prev_quantity,
        LAG(quantity, 1) OVER (PARTITION BY order_id ORDER BY greatest_quantity_unit_price, id) AS prev_quantity_2,
        CORR(unit_price, quantity) OVER (PARTITION BY channel, order_id) AS corr_unit_price_quantity
    FROM joined
),
enriched_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        unit_price,
        MAX(prev_quantity) OVER w_recent AS last3_max_prev_quantity,
        COUNT(prev_quantity) OVER w_recent AS last3_count_prev_quantity,
        MIN(prev_quantity) OVER w_recent AS last3_min_prev_quantity
    FROM enriched
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY prev_quantity_2, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(last3_max_prev_quantity AS INT) AS last3_max_prev_quantity_int,
        last3_count_prev_quantity - last3_max_prev_quantity AS last3_count_prev_quantity_minus_last3_max_prev_quantity
    FROM enriched_order_items
    WHERE last3_count_prev_quantity <= 14
)
SELECT
    id,
    unit_price,
    last3_max_prev_quantity,
    last3_count_prev_quantity,
    last3_min_prev_quantity,
    last3_count_prev_quantity_minus_last3_max_prev_quantity
FROM valid
ORDER BY last3_min_prev_quantity DESC;
