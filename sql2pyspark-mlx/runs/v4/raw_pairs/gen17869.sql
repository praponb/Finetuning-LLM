WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, v
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        level,
        key,
        v,
        INITCAP(TRIM(service)) AS service_title
    FROM lines
    WHERE service = 'search'
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(level) AS level_upper
    FROM filtered
    WHERE level = 'WARN'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        level,
        key,
        service_title,
        level_upper,
        INITCAP(TRIM(level)) AS level_title,
        CASE WHEN level IN ('ERROR', 'INFO') THEN 'Core' ELSE 'Other' END AS level_group
    FROM base
    WHERE level NOT IN ('ERROR', 'WARN', 'DEBUG')
)
SELECT
    id,
    level,
    key,
    service_title,
    level_title,
    level_group
FROM valid
ORDER BY level DESC, key
