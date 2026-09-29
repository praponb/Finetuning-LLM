WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, v
),
flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        level,
        latency_ms,
        attr_key,
        v,
        CAST((status_code & 4) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM flattened_attrs
    INTERSECT
    SELECT id
    FROM app_logs
    WHERE service IN ('billing', 'auth', 'checkout')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened_attrs s
    JOIN remaining_keys k ON s.id = k.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        level,
        latency_ms,
        attr_key,
        v,
        has_fault,
        status_code_high,
        UPPER(level) AS level_upper,
        LEAST(latency_ms, has_fault) AS least_latency_ms_has_fault
    FROM kept_rows
    WHERE attr_key IS NOT NULL
      AND latency_ms < 1784
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        level,
        latency_ms,
        attr_key,
        status_code_high,
        level_upper,
        least_latency_ms_has_fault,
        CASE WHEN service = 'checkout' THEN 1 ELSE 0 END AS is_checkout,
        COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM base
    WHERE attr_key IS DISTINCT FROM 'region'
)
SELECT
    id,
    service,
    level,
    latency_ms,
    attr_key,
    status_code_high,
    level_upper,
    least_latency_ms_has_fault,
    is_checkout,
    CASE WHEN attr_key RLIKE '^(trace|version)$' THEN 'OTHER_ZONE' WHEN attr_key RLIKE '^(region)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS attr_key_zone
FROM valid
ORDER BY id DESC
