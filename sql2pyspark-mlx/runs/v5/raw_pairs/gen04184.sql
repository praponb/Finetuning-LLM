WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
),
unpacked AS (
    SELECT
        id,
        level,
        attr_key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS has_fault
    FROM flattened
    WHERE level != 'INFO'
),
enriched AS (
    SELECT
        id,
        attr_key,
        has_fault,
        FIRST_VALUE(has_fault, TRUE) OVER (PARTITION BY attr_key ORDER BY status_code_high, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_has_fault,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code_high DESC, id, attr_key) AS seq_no,
        AVG(status_code_high) OVER (PARTITION BY attr_key ORDER BY has_fault, id, attr_key ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS status_code_high_avg_5_prior,
        MAX(status_code_high) OVER (PARTITION BY level) AS level_max_status_code_high
    FROM unpacked
),
cleaned AS (
    SELECT
        *,
        CASE WHEN status_code_high_avg_5_prior >= 3 THEN 'High' WHEN status_code_high_avg_5_prior >= 1 THEN 'Medium' ELSE 'Low' END AS status_code_high_avg_5_prior_band,
        COALESCE(attr_key, 'DIRECT') AS attr_key_filled
    FROM enriched
    WHERE attr_key IS NOT NULL
      AND status_code_high_avg_5_prior IS NULL
),
with_windows AS (
    SELECT
        *,
        MIN(level_max_status_code_high) OVER (PARTITION BY attr_key ORDER BY level_max_status_code_high, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_level_max_status_code_high,
        SUM(level_max_status_code_high) OVER (PARTITION BY attr_key ORDER BY level_max_status_code_high, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_level_max_status_code_high,
        AVG(level_max_status_code_high) OVER (PARTITION BY attr_key ORDER BY level_max_status_code_high, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_level_max_status_code_high,
        LAST_VALUE(first_known_has_fault, TRUE) OVER (PARTITION BY attr_key ORDER BY has_fault, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_known_has_fault
    FROM cleaned
    WHERE first_known_has_fault IS NOT NULL
)
SELECT
    id,
    attr_key,
    has_fault,
    status_code_high_avg_5_prior,
    level_max_status_code_high,
    attr_key_filled,
    running_sum_level_max_status_code_high,
    running_avg_level_max_status_code_high,
    last_known_first_known_has_fault
FROM with_windows
ORDER BY level_max_status_code_high DESC, has_fault
