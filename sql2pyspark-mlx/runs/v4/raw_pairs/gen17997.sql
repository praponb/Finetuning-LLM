WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
),
flattened_attrs AS (
    SELECT
        id,
        log_ts,
        k,
        value,
        CAST((status_code & 2) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE k IN ('trace', 'region')
),
metrics AS (
    SELECT
        *,
        MIN(has_fault) OVER (PARTITION BY k) AS k_min_has_fault,
        COUNT(has_fault) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS has_fault_count_1h,
        SUM(status_code_high) OVER (PARTITION BY k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_high_sum_30m,
        COUNT(has_fault) OVER (PARTITION BY k) AS k_count_has_fault
    FROM flattened_attrs
)
SELECT
    id,
    log_ts,
    k,
    has_fault,
    k_min_has_fault,
    status_code_high_sum_30m,
    k_count_has_fault,
    CASE WHEN k RLIKE '^(trace|region)$' THEN 'EXTENDED' WHEN k RLIKE '^(version)$' THEN 'GROUP_B' ELSE 'CORE' END AS k_zone
FROM metrics
ORDER BY log_ts DESC NULLS LAST
