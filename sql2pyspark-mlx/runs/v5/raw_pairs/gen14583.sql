WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, v
),
parsed AS (
    SELECT
        status_code,
        attr_key,
        v,
        CAST((status_code & 8) != 0 AS INT) AS flag_bit8,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE status_code = 293
),
with_windows AS (
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share,
        DENSE_RANK() OVER (ORDER BY status_code_high) AS status_code_high_dense_rank
    FROM parsed
),
with_windows_app_logs AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY attr_key ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_rank,
        SUM(flag_bit8) OVER (PARTITION BY attr_key) AS attr_key_sum_flag_bit8
    FROM with_windows
)
SELECT
    attr_key,
    flag_bit8,
    status_code_high,
    status_code_high_dense_rank,
    status_code_high_rank,
    attr_key_sum_flag_bit8
FROM with_windows_app_logs;
