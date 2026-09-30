WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        attr_key,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS has_fault
    FROM flattened
    WHERE attr_key IN ('region', 'trace')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        attr_key,
        attr_value,
        status_code_high,
        has_fault,
        FLOOR(has_fault) AS has_fault_whole,
        CASE WHEN has_fault >= 1 THEN 'High' WHEN has_fault >= 0 THEN 'Medium' ELSE 'Low' END AS has_fault_level
    FROM unpacked
    WHERE status_code_high BETWEEN 1 AND 2
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(has_fault_whole) OVER (PARTITION BY attr_key ORDER BY has_fault_whole, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_has_fault_whole,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_cume_dist,
        NTILE(5) OVER (ORDER BY has_fault DESC, id, attr_key) AS has_fault_bucket,
        LAG(status_code_high, 1) OVER (PARTITION BY attr_key ORDER BY has_fault, id, attr_key) AS prev_status_code_high
    FROM valid
    WHERE status_code_high <= 2
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        attr_key,
        attr_value,
        has_fault_whole,
        cumulative_has_fault_whole,
        status_code_high_cume_dist,
        prev_status_code_high
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY attr_key ORDER BY status_code_high_cume_dist) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        attr_key,
        attr_value,
        LEAD(status_code_high_cume_dist) OVER w_hist AS next_status_code_high_cume_dist,
        LEAD(has_fault_whole) OVER w_hist AS next_has_fault_whole,
        ROW_NUMBER() OVER w_hist AS event_seq
    FROM latest
    WINDOW w_hist AS (PARTITION BY attr_key ORDER BY cumulative_has_fault_whole, id, attr_key)
)
SELECT
    id,
    attr_key,
    attr_value,
    next_status_code_high_cume_dist,
    next_has_fault_whole,
    CASE WHEN attr_key RLIKE '^(trace|version)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS attr_key_zone
FROM metrics;
