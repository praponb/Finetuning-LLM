WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        k,
        value,
        CASE WHEN status_code >= 412 THEN 'High' WHEN status_code >= 278 THEN 'Medium' ELSE 'Low' END AS status_code_level,
        COALESCE(k, 'NONE') AS k_filled
    FROM lines
    WHERE host = 'h2'
),
valid_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(k, 'Unknown') AS k_filled_2,
        COALESCE(k, 'OTHER') AS resolved_k
    FROM valid
    WHERE k NOT IN ('region', 'version')
)
SELECT
    id,
    k,
    value,
    status_code_level,
    k_filled,
    CASE WHEN k RLIKE '^(trace|region)$' THEN 'GROUP_B' WHEN k RLIKE '^(version)$' THEN 'EXTENDED' ELSE 'CORE' END AS k_zone
FROM valid_app_logs
ORDER BY k_zone NULLS FIRST
