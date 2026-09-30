WITH exploded AS (
    SELECT
        id,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, attr_value
),
with_windows AS (
    SELECT
        *,
        MAX(status_code) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_max_30m,
        COUNT(status_code) OVER (PARTITION BY attr_key) AS attr_key_count_status_code,
        COUNT(status_code) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_count_2h
    FROM exploded
),
filtered AS (
    SELECT
        *,
        LEAST(status_code, status_code_count_2h) AS least_status_code_status_code_count_2h,
        attr_key_count_status_code - status_code_count_2h AS attr_key_count_status_code_minus_status_code_count_2h
    FROM with_windows
    WHERE log_ts >= DATE '2025-06-01'
      AND attr_key IS NOT NULL
),
scored AS (
    SELECT
        *,
        AVG(attr_key_count_status_code) OVER (PARTITION BY level) AS level_avg_attr_key_count_status_code,
        MAX(least_status_code_status_code_count_2h) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS least_status_code_status_code_count_2h_max_15m
    FROM filtered
),
base AS (
    SELECT *, ROUND(level_avg_attr_key_count_status_code / status_code, 2) AS level_avg_attr_key_count_status_code_per_status_code
    FROM scored
    WHERE log_ts >= DATE '2025-06-01'
)
SELECT
    id,
    log_ts,
    level,
    status_code,
    attr_value,
    status_code_max_30m,
    attr_key_count_status_code,
    status_code_count_2h,
    attr_key_count_status_code_minus_status_code_count_2h
FROM base
ORDER BY log_ts NULLS FIRST, status_code_max_30m DESC
