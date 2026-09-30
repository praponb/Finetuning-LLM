WITH valid_order_items AS (
    SELECT *, CASE WHEN unit_price >= 370 THEN 'High' WHEN unit_price >= 100 THEN 'Medium' ELSE 'Low' END AS unit_price_tier
    FROM order_items
    WHERE product_id IS NOT NULL
),
summary AS (
    SELECT order_id, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM valid_order_items
    GROUP BY order_id
)
SELECT order_id, quantity_range
FROM summary
WHERE quantity_range < 73
ORDER BY 1, quantity_range DESC
LIMIT 5
