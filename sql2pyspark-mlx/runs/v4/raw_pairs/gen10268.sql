WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, attr_value
),
unpacked AS (
    SELECT
        service,
        host,
        log_ts,
        level,
        key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged
    FROM exploded
    WHERE key IN ('version', 'trace', 'region')
),
scored AS (
    SELECT
        *,
        COUNT(is_flagged) OVER w_part level_count_is_flagged,
        PERCENT_RANK() OVER w_grp status_code_high_percent_rank
    FROM unpacked
    WINDOW w_part AS (PARTITION BY level), w_grp AS (PARTITION BY service ORDER BY status_code_high DESC NULLS FIRST)
)
SELECT
    log_ts,
    level,
    key,
    level_count_is_flagged,
    status_code_high_percent_rank
FROM scored
ORDER BY level DESC NULLS LAST;
