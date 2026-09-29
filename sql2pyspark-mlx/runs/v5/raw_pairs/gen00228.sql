WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, order_id
    FROM order_items
    EXCEPT DISTINCT
    SELECT id, order_id
    FROM order_items
    WHERE quantity = 7
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.order_id = k.order_id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN unit_price >= 340 THEN 'HIGH' ELSE 'LOW' END AS unit_price_tier,
    AVG(quantity) AS avg_quantity,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(quantity, 0.95) AS quantity_p95,
    BIT_OR(quantity) AS quantity_bit_or,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    SUM(unit_price) AS total_unit_price
FROM reconciled
GROUP BY CASE WHEN unit_price >= 340 THEN 'HIGH' ELSE 'LOW' END
ORDER BY num_rows DESC NULLS LAST
