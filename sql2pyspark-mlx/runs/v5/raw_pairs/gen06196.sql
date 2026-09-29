WITH flattened AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
),
enriched AS (
    SELECT
        host,
        level,
        latency_ms,
        status_code,
        key,
        DENSE_RANK() OVER w_hist AS status_code_dense_rank,
        SUM(latency_ms) OVER w_ordered AS latency_ms_sum_1h
    FROM flattened
    WINDOW w_hist AS (ORDER BY status_code DESC), w_ordered AS (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    SELECT *, ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share
    FROM enriched
),
filtered AS (
    SELECT
        host,
        latency_ms,
        status_code,
        key,
        status_code_dense_rank,
        latency_ms_sum_1h,
        ROUND(status_code * 100, 2) AS status_code_scaled
    FROM with_windows
    WHERE latency_ms_sum_1h = 11465
)
SELECT
    host,
    latency_ms,
    key,
    status_code_dense_rank,
    status_code_scaled
FROM filtered
ORDER BY status_code_dense_rank DESC;
