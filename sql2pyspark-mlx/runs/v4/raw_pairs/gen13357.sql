WITH flattened AS (
    SELECT
        id,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, v
)
SELECT
    id,
    k,
    CASE WHEN k RLIKE '^(device|weight_kg)$' THEN 'GROUP_B' WHEN k RLIKE '^(utm_source|gift)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS k_zone
FROM flattened;
