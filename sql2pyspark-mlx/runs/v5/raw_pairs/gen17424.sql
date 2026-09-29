WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        created_at,
        items,
        ship_to,
        attributes['device'] AS device,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
        COALESCE(attributes['gift'], 'unknown') AS gift_or_default,
        attributes['utm_source'] AS utm_source,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'device') AS has_device
    FROM carts
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    MIN(created_at) AS first_created_at,
    ROUND(AVG(weight_kg), 2) AS avg_weight_kg,
    ROUND(AVG(weight_kg), 2) AS avg_weight_kg_2,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(weight_kg, ARRAY(0.25, 0.5, 0.75)) AS weight_kg_quartiles
FROM flattened_attrs
GROUP BY 1
HAVING distinct_ids < 1000
ORDER BY avg_weight_kg ASC NULLS LAST
