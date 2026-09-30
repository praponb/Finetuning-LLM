WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
)
SELECT
    id,
    level,
    attr_key,
    attr_value,
    CASE WHEN attr_key RLIKE '^(trace|version)$' THEN 'OTHER_ZONE' WHEN attr_key RLIKE '^(region)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS attr_key_zone
FROM flattened
