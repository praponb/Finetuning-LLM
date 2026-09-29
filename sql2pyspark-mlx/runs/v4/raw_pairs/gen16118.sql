WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(quantity AS DOUBLE) AS quantity_str
    FROM order_items
    WHERE product_id IN (14, 38)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(quantity_str / quantity, 2) AS quantity_str_per_quantity,
        ROUND(quantity_str / quantity, 2) AS quantity_str_per_quantity_2
    FROM cleaned
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Attach products attributes
    SELECT e.*, b.cost
    FROM base e
    INNER JOIN products b
        ON e.product_id = b.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.amount,
        o.order_date
    FROM enriched src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity_str,
        quantity_str_per_quantity,
        quantity_str_per_quantity_2,
        cost,
        amount,
        order_date,
        LAST_VALUE(quantity_str_per_quantity_2, TRUE) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_str_per_quantity_2,
        SUM(unit_price) OVER (PARTITION BY product_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price
    FROM with_dims
)
SELECT
    id,
    order_id,
    product_id,
    quantity,
    quantity_str,
    quantity_str_per_quantity,
    quantity_str_per_quantity_2,
    cost,
    amount
FROM with_windows
ORDER BY quantity_str_per_quantity_2 NULLS FIRST
