WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        service,
        log_ts,
        latency_ms,
        attr_key,
        attr_value,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        service,
        log_ts,
        latency_ms,
        attr_key,
        flag_bit1,
        CASE WHEN service IN ('auth', 'billing') THEN 'Group A' ELSE 'Other' END AS service_group,
        COALESCE(attr_key, 'UNKNOWN') AS resolved_attr_key
    FROM parsed
    WHERE log_ts >= TIMESTAMP '2025-06-01 06:00:00'
      AND latency_ms BETWEEN 1534 AND 1710
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    AVG(flag_bit1) AS avg_flag_bit1,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list
FROM valid
GROUP BY hour_window
ORDER BY hour_window DESC;
