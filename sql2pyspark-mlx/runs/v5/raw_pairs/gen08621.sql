WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, v
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        level,
        attr_key,
        v,
        SUBSTRING(attr_key, 1, 4) AS attr_key_prefix
    FROM flattened
    WHERE level != 'ERROR'
),
base_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(service) AS service_lower,
        COALESCE(attr_key, 'UNKNOWN') AS resolved_attr_key
    FROM base
    WHERE service != 'auth'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(attr_key, ' ', service) AS attr_key_service,
        COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM base_app_logs
    WHERE service NOT IN ('search', 'billing', 'checkout')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(service)) AS service_title
    FROM valid
    WHERE service = 'search'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        level,
        attr_key,
        v,
        service_lower,
        attr_key_filled,
        service_title,
        UPPER(service) AS service_upper,
        COALESCE(attr_key, 'NONE') AS attr_key_filled_2
    FROM cleaned
    WHERE attr_key IS NOT DISTINCT FROM 'region'
),
cleaned_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        level,
        attr_key,
        service_lower,
        attr_key_filled,
        service_title,
        LOWER(level) AS level_lower,
        COALESCE(attr_key, 'N/A') AS attr_key_filled_3
    FROM filtered
    WHERE attr_key IS NOT NULL
),
stats AS (
    -- Multi-level aggregation
    SELECT
        level,
        COUNT(*) FILTER (WHERE level = 'DEBUG') AS debug_rows,
        COUNT(*) FILTER (WHERE service = 'checkout') AS checkout_rows,
        COUNT_IF(level = 'ERROR') AS error_events
    FROM cleaned_app_logs
    GROUP BY 1
    HAVING debug_rows > 1
)
SELECT
    level,
    debug_rows,
    CASE WHEN level RLIKE '^(ERROR|DEBUG)$' THEN 'GROUP_B' WHEN level RLIKE '^(INFO|WARN)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS level_zone
FROM stats
ORDER BY level_zone ASC, debug_rows ASC
