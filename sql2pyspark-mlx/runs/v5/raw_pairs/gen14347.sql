WITH flattened_attrs AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        ship_to,
        attributes,
        FORALL(items, li -> li.qty <= 5) AS all_small_qty,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
        ship_to.zip AS ship_zip,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'device') AS has_device
    FROM carts
)
SELECT
    CASE WHEN weight_kg >= 3.7 THEN 'PREMIUM' ELSE 'STANDARD' END AS weight_kg_tier,
    ROUND(AVG(weight_kg), 2) AS avg_weight_kg,
    PERCENTILE_APPROX(weight_kg, ARRAY(0.25, 0.5, 0.75)) AS weight_kg_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(weight_kg, 0.95) AS weight_kg_p95
FROM flattened_attrs
GROUP BY CASE WHEN weight_kg >= 3.7 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY weight_kg_tier NULLS LAST;
