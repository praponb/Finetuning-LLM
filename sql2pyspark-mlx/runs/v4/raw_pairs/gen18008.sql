WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        CASE WHEN unit_price >= 380 THEN 'High' WHEN unit_price >= 200 THEN 'Medium' ELSE 'Low' END AS unit_price_level
    FROM order_items
    WHERE product_id IS NULL
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    CASE WHEN unit_price >= 370 THEN 'LARGE' ELSE 'SMALL' END AS unit_price_tier,
    STDDEV_SAMP(unit_price) AS unit_price_stddev,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(unit_price, 0.5) AS unit_price_p50
FROM valid
GROUP BY 1, 2
HAVING unit_price_p50 < 0
ORDER BY unit_price_p50 DESC
