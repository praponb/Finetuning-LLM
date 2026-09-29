WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT order_id
    FROM order_items
    INTERSECT ALL
    SELECT order_id
    FROM order_items
    WHERE order_id IN (38, 39)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id
)
-- Multi-level aggregation
SELECT
    order_id,
    CASE WHEN quantity >= 7 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    STDDEV_SAMP(unit_price) AS unit_price_stddev,
    ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM kept_rows
GROUP BY order_id, CASE WHEN quantity >= 7 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY quantity_tier NULLS LAST;
