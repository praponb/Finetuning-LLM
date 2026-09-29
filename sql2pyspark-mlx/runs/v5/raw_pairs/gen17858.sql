WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        unit_price,
        unit_price * quantity AS line_total,
        ROUND(quantity * 1.15, 2) AS quantity_adjusted
    FROM order_items
    WHERE product_id IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN unit_price >= 380 THEN 'HIGH' ELSE 'LOW' END AS unit_price_tier,
    MIN(unit_price) AS min_unit_price,
    APPROX_PERCENTILE(quantity_adjusted, ARRAY(0.25, 0.5, 0.75)) AS quantity_adjusted_quartiles,
    SUM(unit_price) AS total_unit_price
FROM valid
GROUP BY unit_price_tier
ORDER BY total_unit_price DESC;
