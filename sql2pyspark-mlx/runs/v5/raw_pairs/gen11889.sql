WITH lines AS (
    SELECT
        id,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
),
unpacked AS (
    SELECT
        id,
        level,
        status_code,
        attr_key,
        attr_value,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE attr_key NOT IN ('region', 'version')
),
filtered AS (
    SELECT *, LENGTH(attr_key) AS attr_key_length
    FROM unpacked
    WHERE attr_key IS NOT NULL
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY attr_key_length DESC NULLS FIRST, id, attr_key) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
)
SELECT
    id,
    level,
    status_code,
    attr_key,
    attr_value,
    status_code_high,
    attr_key_length,
    CASE WHEN attr_key RLIKE '^(version|trace)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS attr_key_zone
FROM latest;
