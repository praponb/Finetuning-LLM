WITH flattened AS (
    SELECT
        id,
        created_at,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS attr_key, attr_value
)
SELECT
    id,
    created_at,
    attr_key,
    attr_value,
    CASE WHEN attr_key RLIKE '^(utm_source|weight_kg)$' THEN 'GROUP_B' WHEN attr_key RLIKE '^(device|gift)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS attr_key_zone
FROM flattened
ORDER BY created_at
