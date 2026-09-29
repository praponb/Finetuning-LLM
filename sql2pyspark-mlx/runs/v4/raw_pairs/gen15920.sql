WITH base AS (
    -- Keep recent, valid rows
    SELECT *, GREATEST(unit_price, quantity) AS greatest_unit_price_quantity
    FROM order_items
    WHERE order_id IN (6, 21, 30)
)
-- Multi-level aggregation
SELECT
    CASE WHEN greatest_unit_price_quantity >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END AS greatest_unit_price_quantity_tier,
    CASE WHEN unit_price >= 320 THEN 'LARGE' ELSE 'SMALL' END AS unit_price_tier,
    STDDEV_SAMP(unit_price) AS unit_price_stddev,
    AVG(quantity) AS avg_quantity,
    ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM base
GROUP BY GROUPING SETS ((CASE WHEN greatest_unit_price_quantity >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN unit_price >= 320 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN greatest_unit_price_quantity >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
HAVING ROUND(AVG(unit_price), 2) > 0;
