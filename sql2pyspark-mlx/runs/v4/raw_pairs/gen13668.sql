WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, v
),
unpacked AS (
    -- Semi-structured parsing
    SELECT
        host,
        log_ts,
        status_code,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS has_fault
    FROM lines
    WHERE log_ts >= '2024-10-17'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_count_1h,
        RANK() OVER (ORDER BY has_fault DESC) AS has_fault_rank,
        STDDEV_SAMP(status_code) OVER (PARTITION BY host) AS host_stddev_samp_status_code,
        PERCENT_RANK() OVER (PARTITION BY host ORDER BY has_fault) AS has_fault_percent_rank,
        RANK() OVER (PARTITION BY host ORDER BY has_fault DESC) AS has_fault_rank_2
    FROM unpacked
),
enriched_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_hist AS has_fault_cume_dist,
        COUNT(has_fault_rank_2) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS has_fault_rank_2_count_1h,
        RANK() OVER (PARTITION BY host ORDER BY has_fault_rank_2 NULLS FIRST) AS has_fault_rank_2_rank
    FROM enriched
    WINDOW w_hist AS (PARTITION BY host ORDER BY has_fault DESC)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        has_fault_rank_2_count_1h / NULLIF(has_fault, 0) AS has_fault_rank_2_count_1h_per_has_fault,
        LENGTH(host) AS host_length
    FROM enriched_app_logs
    WHERE host = 'h1'
)
SELECT
    host,
    log_ts,
    status_code,
    v,
    host_stddev_samp_status_code,
    has_fault_cume_dist,
    has_fault_rank_2_count_1h,
    has_fault_rank_2_rank,
    host_length,
    CASE WHEN host RLIKE '^(h2|h1)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS host_zone
FROM cleaned
ORDER BY host_length
