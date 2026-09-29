WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        log_ts,
        status_code,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8
    FROM exploded
    WHERE status_code < 324
),
scored AS (
    -- Ranking and gap features
    SELECT
        log_ts,
        status_code,
        COUNT(status_code_high) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS status_code_high_count_15m
    FROM parsed
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(status_code_high_count_15m) AS status_code_high_count_15m_whole
    FROM scored
    WHERE status_code BETWEEN 358 AND 405
)
SELECT
    log_ts,
    status_code,
    status_code_high_count_15m,
    status_code_high_count_15m_whole
FROM cleaned
ORDER BY log_ts DESC NULLS FIRST;
