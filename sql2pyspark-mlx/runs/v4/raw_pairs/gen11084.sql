SELECT
    product_id,
    CASE WHEN units >= 39 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_tier,
    COUNT(*) AS event_count,
    BIT_OR(units) AS units_bit_or,
    APPROX_PERCENTILE(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles
FROM store_sales
GROUP BY 1, 2
HAVING COUNT(*) < 1
ORDER BY units_tier NULLS LAST;
