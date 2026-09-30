WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(unit_price, 2) AS unit_price_rounded
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity = 3
)
-- Grouped metrics
SELECT
    product_id,
    AVG(unit_price) AS avg_unit_price,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    COUNT(*) AS row_count
FROM valid
GROUP BY product_id
ORDER BY avg_unit_price DESC NULLS FIRST
