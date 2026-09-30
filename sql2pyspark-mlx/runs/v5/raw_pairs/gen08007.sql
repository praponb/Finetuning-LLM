WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS element
)
-- Multi-level aggregation
SELECT
    CASE WHEN element >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END AS element_tier,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(element, ARRAY(0.25, 0.5, 0.75)) AS element_quartiles,
    PERCENTILE_APPROX(element, ARRAY(0.25, 0.5, 0.75)) AS element_quartiles_2,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    MIN(element) AS min_element
FROM flattened
GROUP BY 1
HAVING num_rows >= 1
ORDER BY distinct_ids_2 NULLS LAST;
