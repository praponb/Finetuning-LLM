WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LEAST(unit_price, quantity) AS least_unit_price_quantity,
        CASE WHEN unit_price >= 310 THEN 'High' WHEN unit_price >= 90 THEN 'Medium' ELSE 'Low' END AS unit_price_level
    FROM order_items
    WHERE product_id IS NOT NULL
)
SELECT order_id
FROM valid
INTERSECT ALL
SELECT order_id
FROM order_items
WHERE order_id IN (2, 29, 31, 36);
