WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        COALESCE(quantity, 0) AS quantity_filled,
        FLOOR(unit_price) AS unit_price_whole
    FROM order_items
    WHERE product_id IN (7, 12, 25)
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.quantity,
        src.unit_price,
        src.quantity_filled,
        src.unit_price_whole,
        b.amount
    FROM valid src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
enriched AS (
    -- Attach orders attributes
    SELECT s.*, b.discount
    FROM joined s
    INNER JOIN orders b
        ON s.order_id = b.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        quantity,
        quantity_filled,
        unit_price_whole,
        amount,
        CAST(unit_price AS INT) AS unit_price_int,
        ROUND(discount, 2) AS discount_rounded
    FROM enriched
    WHERE discount >= 0.22
)
SELECT
    s.id,
    s.order_id,
    s.quantity,
    s.quantity_filled,
    s.unit_price_whole,
    s.amount,
    s.unit_price_int,
    s.discount_rounded,
    (SELECT MIN(x.unit_price) FROM order_items x WHERE x.order_id = s.order_id) AS min_order_item_unit_price
FROM filtered s
