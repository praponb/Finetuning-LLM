WITH lines AS (
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
flattened_attrs AS (
    SELECT
        id,
        level,
        status_code,
        attr_key,
        attr_value,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
    WHERE attr_key != 'version'
),
valid AS (
    SELECT
        *,
        LENGTH(level) AS level_length,
        ROUND(is_flagged, 1) AS is_flagged_rounded
    FROM flattened_attrs
    WHERE attr_key IS NOT NULL
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY is_flagged ASC, id ASC, attr_key ASC) AS row_num
        FROM valid
    ) x
    WHERE row_num = 1
),
metrics AS (
    SELECT
        *,
        SUM(is_flagged_rounded) OVER (PARTITION BY level ORDER BY is_flagged ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_flagged_rounded,
        FIRST_VALUE(status_code_high) IGNORE NULLS OVER (PARTITION BY attr_key ORDER BY is_flagged ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_code_high
    FROM deduped
    WHERE status_code_high IS NOT NULL
),
cleaned AS (
    SELECT *, CAST(status_code AS STRING) AS status_code_num
    FROM metrics
    WHERE attr_key IS DISTINCT FROM 'version'
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY first_known_status_code_high DESC NULLS LAST, id ASC, attr_key ASC) AS dedup_rank
    FROM cleaned
    QUALIFY dedup_rank = 1
),
summary AS (
    SELECT
        attr_key,
        ROUND(AVG(is_flagged_rounded), 2) AS avg_is_flagged_rounded,
        APPROX_PERCENTILE(cumulative_is_flagged_rounded, 0.95) AS cumulative_is_flagged_rounded_p95,
        PERCENTILE_APPROX(is_flagged, 0.5) AS is_flagged_p50,
        COUNT(DISTINCT level) AS distinct_levels,
        ROUND(AVG(status_code_high), 2) AS avg_status_code_high
    FROM latest
    GROUP BY 1
)
SELECT
    attr_key,
    is_flagged_p50,
    distinct_levels,
    avg_status_code_high,
    CASE WHEN attr_key RLIKE '^(version|trace)$' THEN 'GROUP_A' WHEN attr_key RLIKE '^(region)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS attr_key_zone
FROM summary
