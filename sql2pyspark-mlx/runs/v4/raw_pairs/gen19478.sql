WITH flattened AS (
    SELECT
        id,
        host,
        log_ts,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
),
flattened_attrs AS (
    SELECT
        id,
        attr_key,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
valid AS (
    SELECT *, GREATEST(status_code_high, flag_bit2) AS greatest_status_code_high_flag_bit2
    FROM flattened_attrs
    WHERE attr_key IS NOT NULL
      AND status_code_high <= 2
),
filtered AS (
    SELECT
        *,
        LOWER(attr_key) AS attr_key_lower,
        COALESCE(attr_key, 'UNKNOWN') AS attr_key_filled
    FROM valid
    WHERE greatest_status_code_high_flag_bit2 < 35
)
SELECT
    id,
    attr_key,
    flag_bit2,
    status_code_high,
    greatest_status_code_high_flag_bit2,
    attr_key_lower,
    attr_key_filled,
    CASE WHEN attr_key RLIKE '^(trace|region)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS attr_key_zone
FROM filtered
ORDER BY status_code_high DESC, attr_key DESC;
