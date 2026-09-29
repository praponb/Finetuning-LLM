WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, value
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        key,
        value
    FROM flattened
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY status_code DESC, id ASC, key ASC) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(key, 'unassigned') AS key_filled
    FROM deduped
    WHERE key = 'trace'
      AND key IS DISTINCT FROM 'trace'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(key, 1, 4) AS key_prefix,
        CASE WHEN key IN ('region', 'version') THEN 'Group A' ELSE 'Other' END AS key_group
    FROM base
    WHERE key IS NOT NULL
      AND key != 'version'
),
cleaned_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        key,
        value,
        key_filled,
        key_prefix,
        key_group,
        INITCAP(TRIM(key)) AS key_title
    FROM cleaned
    WHERE key NOT IN ('trace', 'region')
)
-- Aggregate with conditional metrics
SELECT
    key,
    COUNT(*) FILTER (WHERE key = 'version') AS version_rows,
    COUNT(CASE WHEN key = 'region' THEN 1 END) AS region_count,
    COUNT_IF(key = 'region') AS region_events,
    COUNT(DISTINCT key) AS distinct_keys,
    COUNT(DISTINCT id) AS distinct_ids
FROM cleaned_app_logs
GROUP BY key
ORDER BY region_count ASC;
