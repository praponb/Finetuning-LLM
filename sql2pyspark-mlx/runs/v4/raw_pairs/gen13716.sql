WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT id, element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS element
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN element >= 80 THEN 'HIGH' ELSE 'LOW' END AS element_tier,
    ROUND(AVG(element), 2) AS avg_element,
    PERCENTILE_APPROX(element, 0.9) AS element_p90,
    APPROX_PERCENTILE(element, ARRAY(0.25, 0.5, 0.75)) AS element_quartiles,
    PERCENTILE_APPROX(element, ARRAY(0.25, 0.5, 0.75)) AS element_quartiles_2
FROM exploded
GROUP BY CASE WHEN element >= 80 THEN 'HIGH' ELSE 'LOW' END
ORDER BY element_tier NULLS LAST;
