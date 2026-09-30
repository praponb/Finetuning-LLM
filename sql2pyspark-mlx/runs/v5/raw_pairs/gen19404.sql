WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        CAST(unit_price AS INT) AS unit_price_int
    FROM order_items
    WHERE product_id IS NOT NULL
),
joined AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.unit_price,
        src.unit_price_int,
        p.product_name,
        p.price,
        p.cost
    FROM cleaned src
    LEFT JOIN products p
        ON src.product_id = p.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        unit_price,
        unit_price_int,
        product_name,
        price,
        cost,
        COALESCE(unit_price, 0) AS unit_price_filled,
        GREATEST(unit_price_int, cost) AS greatest_unit_price_int_cost
    FROM joined
    WHERE price BETWEEN 560 AND 970
),
matched_keys AS (
    -- Set intersection on keys
    SELECT product_id
    FROM base
    INTERSECT ALL
    SELECT product_id
    FROM order_items
    WHERE order_id = 24
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.product_id = k.product_id
),
enriched AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.product_name AS product_product_name,
        p.cost AS product_cost
    FROM reconciled src
    LEFT JOIN products p
        ON src.product_id = p.id
    WHERE SIZE(p.tags) >= 2
)
SELECT
    id,
    product_id,
    unit_price,
    unit_price_int,
    product_name,
    price,
    cost,
    unit_price_filled,
    greatest_unit_price_int_cost
FROM enriched
ORDER BY id DESC NULLS LAST;
