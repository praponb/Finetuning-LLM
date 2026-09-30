WITH flattened AS (
    SELECT
        id,
        service,
        level,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
),
base AS (
    SELECT
        *,
        UPPER(k) AS k_upper,
        COALESCE(k, 'N/A') AS k_clean
    FROM flattened
    WHERE k IS NOT NULL
),
filtered AS (
    SELECT
        id,
        service,
        level,
        k,
        k_clean,
        CONCAT(k, '/', level) AS k_level
    FROM base
    WHERE level IN ('DEBUG', 'WARN')
)
SELECT
    k,
    level,
    COUNT(*) FILTER (WHERE level = 'DEBUG') AS debug_rows,
    SORT_ARRAY(COLLECT_SET(service)) AS service_list,
    COUNT(DISTINCT level) AS distinct_levels,
    RANK() OVER (PARTITION BY k ORDER BY COUNT(*) FILTER (WHERE level = 'DEBUG') DESC) AS debug_rows_rank
FROM filtered
GROUP BY k, level
