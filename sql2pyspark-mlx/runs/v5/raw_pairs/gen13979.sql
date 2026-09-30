WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        log_ts,
        k,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS is_flagged
    FROM lines
    WHERE host IN ('h3', 'h2')
),
remaining_keys AS (
    -- Set difference on keys
    SELECT host
    FROM parsed
    EXCEPT ALL
    SELECT host
    FROM app_logs
    WHERE log_ts >= '2025-09-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM parsed s
    JOIN remaining_keys k ON s.host = k.host
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, status_code_high * is_flagged status_code_high_times_is_flagged
    FROM kept_rows
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 11:00:00')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(status_code_high) OVER (PARTITION BY host) host_max_status_code_high,
        MIN(status_code_high) OVER (PARTITION BY k, host) k_min_status_code_high,
        CUME_DIST() OVER (PARTITION BY k ORDER BY status_code_high DESC NULLS LAST) status_code_high_cume_dist
    FROM filtered
    WHERE status_code_high_times_is_flagged <= 30
)
SELECT
    k,
    status_code_high,
    is_flagged,
    status_code_high_times_is_flagged,
    host_max_status_code_high,
    CASE WHEN k RLIKE '^(region|version)$' THEN 'GROUP_B' WHEN k RLIKE '^(trace)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS k_zone
FROM with_windows
ORDER BY is_flagged DESC NULLS LAST;
