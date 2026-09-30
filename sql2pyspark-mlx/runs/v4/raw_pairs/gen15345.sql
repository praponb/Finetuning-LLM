WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, v
),
unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        latency_ms,
        attr_key,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS has_fault
    FROM lines
    WHERE attr_key = 'version'
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM unpacked
    EXCEPT
    SELECT id
    FROM app_logs
    WHERE level IN ('WARN', 'DEBUG')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM unpacked s
    JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    service,
    latency_ms,
    attr_key,
    v,
    status_code_high,
    has_fault,
    CASE WHEN attr_key RLIKE '^(trace|version)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS attr_key_zone
FROM reconciled;
