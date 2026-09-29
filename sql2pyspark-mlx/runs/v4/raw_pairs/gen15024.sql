WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS attr_key, value
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        level,
        attr_key,
        INITCAP(TRIM(service)) AS service_title,
        COALESCE(attr_key, 'unassigned') AS attr_key_filled,
        COALESCE(attr_key, 'OTHER') AS resolved_attr_key
    FROM flattened
    WHERE host NOT IN ('h3', 'h1', 'h4')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        level,
        service_title,
        attr_key_filled,
        CONCAT_WS('/', service, level) AS service_level_key
    FROM base
    WHERE attr_key IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        service,
        host,
        level,
        attr_key_filled,
        service_level_key,
        TRIM(host) AS clean_host
    FROM filtered
    WHERE level IN ('INFO', 'ERROR', 'DEBUG')
)
-- Multi-level aggregation
SELECT
    service,
    host,
    COUNT(*) AS cnt,
    COUNT_IF(host = 'h4') AS h4_events,
    COUNT(CASE WHEN level = 'INFO' THEN 1 END) AS info_count,
    COUNT(*) AS num_rows,
    SORT_ARRAY(COLLECT_SET(host)) AS host_list,
    COUNT(CASE WHEN level = 'ERROR' THEN 1 END) AS error_count
FROM cleaned
GROUP BY service, host
