WITH remaining_keys AS (
    -- Set difference on keys
    SELECT order_id, id
    FROM order_items
    EXCEPT ALL
    SELECT order_id, id
    FROM order_items
    WHERE quantity < 6
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    JOIN remaining_keys k ON s.order_id = k.order_id AND s.id = k.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(quantity * 1.15, 2) AS quantity_scaled
    FROM reconciled
    WHERE product_id IS NOT NULL
),
with_dims AS (
    -- Attach products attributes
    SELECT
        s.*,
        b.cost,
        b.price,
        b.brand
    FROM valid s
    LEFT JOIN products b
        ON s.product_id = b.id
    WHERE b.cost BETWEEN 570 AND 610
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        product_id,
        quantity,
        unit_price,
        cost,
        price,
        quantity + price AS quantity_plus_price,
        INITCAP(TRIM(brand)) AS brand_title
    FROM with_dims
    WHERE product_id IS NOT NULL
      AND order_id IN (4, 7, 37)
)
SELECT product_id, cost
FROM filtered
ORDER BY product_id DESC NULLS LAST;
