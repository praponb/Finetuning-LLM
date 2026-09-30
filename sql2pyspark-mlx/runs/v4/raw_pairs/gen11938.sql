WITH exploded AS (
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, v
),
with_windows AS (
    SELECT
        *,
        LEAD(status_code, 1) OVER (PARTITION BY level ORDER BY status_code, id, k) AS next_status_code,
        LAST_VALUE(k, TRUE) OVER w_hist AS last_known_k
    FROM exploded
    WINDOW w_hist AS (ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_app_logs AS (
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        k,
        v,
        next_status_code,
        last_known_k,
        SUM(next_status_code) OVER (PARTITION BY host ORDER BY next_status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_status_code,
        STDDEV_SAMP(status_code) OVER (PARTITION BY k) AS k_stddev_samp_status_code
    FROM with_windows
)
SELECT
    id,
    service,
    host,
    k,
    next_status_code,
    running_next_status_code,
    k_stddev_samp_status_code,
    CASE WHEN k RLIKE '^(version|region)$' THEN 'GROUP_A' WHEN k RLIKE '^(trace)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS k_zone
FROM with_windows_app_logs
ORDER BY k DESC
