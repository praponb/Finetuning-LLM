WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(unit_price, quantity) AS greatest_unit_price_quantity
    FROM order_items
    WHERE product_id IS NOT NULL
      AND product_id IS NULL
)
-- QUALIFY-style window filter
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC NULLS FIRST, id) AS rn
    FROM base
) t
WHERE rn = 1
