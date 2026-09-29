WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(quantity * 100, 2) AS quantity_adjusted,
        CAST(unit_price AS INT) AS unit_price_int
    FROM order_items
    WHERE quantity >= 5
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    product_id,
    metric,
    val
FROM (
    SELECT
        id,
        product_id,
        unit_price,
        quantity_adjusted
    FROM cleaned
) src
UNPIVOT (
    val FOR metric IN (unit_price, quantity_adjusted)
);
