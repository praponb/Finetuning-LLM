WITH cleaned AS (
    SELECT
        *,
        GREATEST(quantity, unit_price) AS greatest_quantity_unit_price,
        GREATEST(quantity, unit_price) AS greatest_quantity_unit_price_2
    FROM order_items
    WHERE product_id IS NOT NULL
      AND unit_price <= 310
)
SELECT product_id
FROM cleaned
INTERSECT ALL
SELECT product_id
FROM order_items
WHERE quantity BETWEEN 5 AND 8;
