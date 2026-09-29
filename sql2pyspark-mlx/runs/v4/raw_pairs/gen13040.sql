WITH exploded AS (
    SELECT
        id,
        log_ts,
        level,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
),
kept_keys AS (
    SELECT id, level
    FROM exploded
    EXCEPT
    SELECT id, level
    FROM app_logs
    WHERE level NOT IN ('DEBUG', 'WARN')
),
verified AS (
    SELECT s.*
    FROM exploded s
    INNER JOIN kept_keys k ON s.id = k.id AND s.level = k.level
),
filtered AS (
    SELECT
        *,
        LENGTH(k) k_length,
        COALESCE(k, 'UNKNOWN') k_filled
    FROM verified
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 06:00:00')
      AND k IS NOT NULL
)
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    ROUND(SUM(k_length) / NULLIF(COUNT(CASE WHEN level = 'INFO' THEN 1 END), 0), 2) AS k_length_per_info,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    SUM(CASE WHEN k = 'region' THEN k_length ELSE 0 END) AS region_k_length,
    SUM(CASE WHEN k = 'trace' THEN k_length ELSE 0 END) AS trace_k_length,
    SIZE(COLLECT_SET(level)) AS num_levels,
    ROUND(SUM(k_length) / NULLIF(COUNT(CASE WHEN level = 'DEBUG' THEN 1 END), 0), 2) AS k_length_per_debug
FROM filtered
GROUP BY 1
HAVING k_length_per_debug >= 0
ORDER BY num_levels NULLS LAST;
