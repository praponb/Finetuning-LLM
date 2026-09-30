WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        level,
        status_code,
        key,
        value,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE level <> 'ERROR'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        level,
        status_code,
        key,
        value,
        is_flagged,
        status_code_high,
        GREATEST(status_code_high, is_flagged) AS greatest_status_code_high_is_flagged,
        CASE WHEN level IN ('DEBUG', 'WARN') THEN 'Priority' ELSE 'Other' END AS level_group
    FROM unpacked
    WHERE service = 'search'
),
filtered_app_logs AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN greatest_status_code_high_is_flagged >= 74 THEN 'High' WHEN greatest_status_code_high_is_flagged >= 39 THEN 'Medium' ELSE 'Low' END AS greatest_status_code_high_is_flagged_tier
    FROM filtered
    WHERE is_flagged > 0
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(status_code_high) OVER w_ordered AS service_avg_status_code_high,
        ROW_NUMBER() OVER w_running AS row_seq,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY greatest_status_code_high_is_flagged DESC NULLS FIRST) AS greatest_status_code_high_is_flagged_dense_rank,
        MAX(greatest_status_code_high_is_flagged) OVER (PARTITION BY service, level) AS service_max_greatest_status_code_high_is_flagged
    FROM filtered_app_logs
    WINDOW w_ordered AS (PARTITION BY service), w_running AS (PARTITION BY level, service ORDER BY status_code DESC, id, key)
),
with_windows_app_logs AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(service_max_greatest_status_code_high_is_flagged, TRUE) OVER (PARTITION BY key ORDER BY greatest_status_code_high_is_flagged_dense_rank, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_service_max_greatest_status_code_high_is_flagged,
        SUM(status_code_high) OVER (ORDER BY row_seq, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code_high
    FROM with_windows
)
-- Window metrics over several different partitions
SELECT
    id,
    service,
    status_code,
    key,
    value,
    is_flagged,
    status_code_high,
    level_group,
    greatest_status_code_high_is_flagged_tier,
    service_avg_status_code_high,
    greatest_status_code_high_is_flagged_dense_rank,
    service_max_greatest_status_code_high_is_flagged,
    last_known_service_max_greatest_status_code_high_is_flagged,
    cumulative_status_code_high,
    AVG(greatest_status_code_high_is_flagged) OVER (PARTITION BY service ORDER BY last_known_service_max_greatest_status_code_high_is_flagged, id, key ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS greatest_status_code_high_is_flagged_avg_6_prior,
    NTILE(4) OVER (PARTITION BY level ORDER BY status_code, id, key) AS status_code_bucket,
    AVG(last_known_service_max_greatest_status_code_high_is_flagged) OVER (ORDER BY greatest_status_code_high_is_flagged, id, key ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last_known_service_max_greatest_status_code_high_is_flagged_avg_3
FROM with_windows_app_logs
