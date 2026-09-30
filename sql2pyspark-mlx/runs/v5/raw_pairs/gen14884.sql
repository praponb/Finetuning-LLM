WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(level, 1, 2) AS level_prefix,
        COALESCE(attr_key, 'NONE') AS attr_key_clean
    FROM flattened
    WHERE attr_key IS NOT NULL
      AND level IN ('INFO', 'WARN', 'DEBUG')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        attr_key,
        level_prefix,
        attr_key_clean,
        TRIM(attr_key) AS clean_attr_key,
        COALESCE(attr_key, 'OTHER') AS attr_key_filled
    FROM valid
    WHERE attr_key IN ('version', 'trace')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        attr_key,
        level_prefix,
        attr_key_clean,
        clean_attr_key,
        CONCAT(service, ' ', attr_key) AS service_attr_key,
        LENGTH(attr_key) AS attr_key_length
    FROM base
    WHERE attr_key IS NOT NULL
      AND service <> 'auth'
)
SELECT
    id,
    service,
    attr_key,
    level_prefix,
    attr_key_clean,
    clean_attr_key,
    service_attr_key,
    attr_key_length
FROM cleaned
ORDER BY attr_key_length DESC NULLS FIRST
