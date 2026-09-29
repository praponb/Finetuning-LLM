WITH filtered AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        ROUND(unit_price / quantity, 2) line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND unit_price > 130
)
SELECT *
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price NULLS LAST, id) = 1
