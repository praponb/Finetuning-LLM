WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, attr_value
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(attr_key, 1, 4) AS attr_key_prefix
    FROM flattened
    WHERE host = 'h2'
),
cleaned_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        attr_key,
        attr_value,
        attr_key_prefix,
        CONCAT_WS('/', attr_key, host) AS attr_key_host_key
    FROM cleaned
    WHERE attr_key IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        attr_key,
        attr_value,
        attr_key_prefix,
        attr_key_host_key,
        CASE WHEN attr_key = 'region' THEN 1 ELSE 0 END AS is_region,
        CASE WHEN attr_key IN ('region', 'trace') THEN 'Core' ELSE 'Other' END AS attr_key_group,
        COALESCE(attr_key, 'DIRECT') AS attr_key_clean
    FROM cleaned_app_logs
    WHERE attr_key IS NOT NULL
      AND attr_key IN ('trace', 'region')
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        host,
        attr_key,
        attr_value,
        attr_key_host_key,
        attr_key_group,
        attr_key_clean,
        CAST(is_region AS DOUBLE) AS is_region_str,
        CAST(is_region AS STRING) AS is_region_str_2,
        COALESCE(attr_key, 'OTHER') AS resolved_attr_key
    FROM filtered
    WHERE is_region = 46
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY is_region_str, id, attr_key) AS row_num
    FROM valid
    QUALIFY row_num = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY attr_key ORDER BY row_num DESC) AS rn
    FROM latest
    QUALIFY rn = 1
)
SELECT
    id,
    host,
    attr_key,
    attr_value,
    attr_key_host_key,
    attr_key_group,
    attr_key_clean,
    is_region_str,
    is_region_str_2
FROM ranked
ORDER BY is_region_str DESC NULLS FIRST
