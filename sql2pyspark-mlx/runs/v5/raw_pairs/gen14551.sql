WITH exploded AS (
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
parsed AS (
    SELECT
        id,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS has_fault
    FROM exploded
),
cleaned AS (
    SELECT *, FLOOR(status_code_high) status_code_high_whole
    FROM parsed
    WHERE has_fault < 0
),
with_windows AS (
    SELECT
        id,
        attr_value,
        CUME_DIST() OVER w_ordered status_code_high_whole_cume_dist,
        DENSE_RANK() OVER (ORDER BY status_code_high DESC NULLS LAST) status_code_high_dense_rank
    FROM cleaned
    WINDOW w_ordered AS (ORDER BY status_code_high_whole NULLS LAST)
),
cleaned_app_logs AS (
    SELECT *, ROUND(status_code_high_dense_rank, 1) status_code_high_dense_rank_rounded
    FROM with_windows
    WHERE status_code_high_dense_rank < 5
)
SELECT
    id,
    attr_value,
    status_code_high_dense_rank
FROM cleaned_app_logs
ORDER BY id DESC NULLS LAST
