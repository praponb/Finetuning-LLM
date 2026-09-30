WITH valid AS (
    SELECT *, ROUND(unit_price / quantity, 2) AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity BETWEEN 3 AND 5
)
SELECT id, product_id
FROM valid
EXCEPT
SELECT id, product_id
FROM order_items
WHERE quantity = 3;
