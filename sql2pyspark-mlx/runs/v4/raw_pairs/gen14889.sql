WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(unit_price, 0) AS unit_price_filled
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity BETWEEN 3 AND 5
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        product_id,
        unit_price,
        unit_price_filled
    FROM valid
) src
UNPIVOT (
    reading FOR metric IN (unit_price, unit_price_filled)
);
