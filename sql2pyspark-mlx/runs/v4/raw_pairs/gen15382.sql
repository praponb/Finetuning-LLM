WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, attr_value
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS(',', level, key) level_key_key
    FROM exploded
    WHERE key IS NOT NULL
      AND host = 'h3'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(host)) host_title,
        CONCAT(key, '/', host) key_host,
        COALESCE(key, 'OTHER') key_filled
    FROM cleaned
    WHERE key IS NOT NULL
      AND key IN ('region', 'trace', 'version')
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        key,
        level_key_key,
        host_title,
        key_filled,
        LOWER(host) host_lower,
        UPPER(key) key_upper
    FROM valid
    WHERE key <> 'version'
),
summary AS (
    -- Multi-level aggregation
    SELECT
        key,
        COUNT(*) FILTER (WHERE key = 'version') AS version_rows,
        SORT_ARRAY(COLLECT_SET(key)) AS key_list
    FROM filtered
    GROUP BY key
    HAVING COUNT(*) FILTER (WHERE key = 'version') > 1
)
SELECT
    key,
    version_rows,
    key_list,
    CASE WHEN key RLIKE '^(trace|version)$' THEN 'OTHER_ZONE' WHEN key RLIKE '^(region)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS key_zone
FROM summary
ORDER BY key;
