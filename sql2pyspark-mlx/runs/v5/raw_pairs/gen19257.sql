WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        quantity,
        COALESCE(unit_price, 0) AS unit_price_filled
    FROM order_items
    WHERE unit_price < 250
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.*,
        d.stock_qty,
        d.launch_date,
        d.product_name
    FROM base s
    INNER JOIN products d
        ON s.product_id = d.id
    WHERE d.price > 620
),
with_dims AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.unit_price_filled,
        s.launch_date,
        p.category,
        p.product_name AS product_product_name
    FROM enriched s
    LEFT JOIN products p
        ON s.product_id <=> p.id
),
with_dims_order_items AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.product_product_name,
        d.stock_qty
    FROM with_dims e
    INNER JOIN products d
        ON e.product_id = d.id
),
enriched_order_items AS (
    -- Attach products attributes
    SELECT
        src.*,
        b.price,
        b.cost
    FROM with_dims_order_items src
    LEFT JOIN products b
        ON src.product_id = b.id
),
enriched_order_items_2 AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.product_product_name,
        src.stock_qty,
        b.category
    FROM enriched_order_items src
    INNER JOIN products b
        ON src.product_id <=> b.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM enriched_order_items_2
    EXCEPT
    SELECT id, product_id
    FROM order_items
    WHERE quantity >= 3
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_order_items_2 s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.product_id = k.product_id
)
-- Running and moving aggregates
SELECT
    product_id,
    product_product_name,
    PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY stock_qty DESC NULLS FIRST) AS stock_qty_percent_rank
FROM reconciled;
