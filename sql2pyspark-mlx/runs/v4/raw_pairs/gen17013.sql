WITH exploded AS (
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
),
parsed AS (
    SELECT
        id,
        latency_ms,
        status_code,
        k,
        CAST((status_code & 8) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE latency_ms BETWEEN 1167 AND 1431
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY latency_ms DESC NULLS LAST, id, k) AS row_num
    FROM parsed
    QUALIFY row_num = 1
)
SELECT
    *,
    NTILE(5) OVER w_running has_fault_bucket,
    CORR(status_code, latency_ms) OVER w_grp corr_status_code_latency_ms,
    MIN(status_code_high) OVER w_grp k_min_status_code_high,
    NTILE(3) OVER w_running has_fault_bucket_2
FROM ranked
WINDOW w_running AS (PARTITION BY k ORDER BY has_fault, id, k), w_grp AS (PARTITION BY k);
