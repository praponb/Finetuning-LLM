WITH remaining_keys AS (
    SELECT id, order_id
    FROM order_items
    EXCEPT
    SELECT id, order_id
    FROM order_items
    WHERE unit_price IS NOT NULL
),
verified AS (
    SELECT s.*
    FROM order_items s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.order_id = k.order_id
)
SELECT
    CASE WHEN unit_price >= 420 THEN 'HIGH' ELSE 'LOW' END AS unit_price_tier,
    SUM(unit_price) AS total_unit_price,
    BIT_AND(quantity) AS quantity_bit_and,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    GROUPING(CASE WHEN unit_price >= 420 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM verified
GROUP BY CUBE (CASE WHEN unit_price >= 420 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY quantity_bit_and DESC NULLS FIRST
