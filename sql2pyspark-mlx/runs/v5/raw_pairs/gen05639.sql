WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value
),
base AS (
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        attr_key,
        value,
        CASE WHEN status_code >= 437 THEN 'High' WHEN status_code >= 298 THEN 'Medium' ELSE 'Low' END status_code_tier,
        COALESCE(attr_key, 'NONE') attr_key_filled
    FROM lines
    WHERE host = 'h2'
),
base_app_logs AS (
    SELECT *, INITCAP(TRIM(attr_key)) attr_key_title
    FROM base
    WHERE attr_key IS NOT NULL
),
filtered AS (
    SELECT
        id,
        service,
        attr_key,
        value,
        status_code_tier,
        attr_key_filled,
        attr_key_title,
        INITCAP(TRIM(level)) level_title,
        LENGTH(level) level_length
    FROM base_app_logs
    WHERE attr_key IS NOT NULL
      AND level = 'ERROR'
),
valid AS (
    SELECT
        *,
        CAST(level_length AS STRING) level_length_str,
        CASE WHEN attr_key IN ('trace', 'version') THEN 'Core' ELSE 'Other' END attr_key_group,
        COALESCE(attr_key, 'OTHER') attr_key_clean
    FROM filtered
    WHERE service = 'auth'
),
metrics AS (
    SELECT
        service,
        attr_key,
        value,
        attr_key_filled,
        attr_key_title,
        level_title,
        level_length,
        attr_key_group,
        attr_key_clean,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY level_length ASC NULLS LAST) level_length_cume_dist
    FROM valid
),
matched_keys AS (
    SELECT service
    FROM metrics
    EXCEPT
    SELECT service
    FROM app_logs
    WHERE service = 'checkout'
),
verified AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.service = k.service
),
stats AS (
    SELECT
        CASE WHEN level_length_cume_dist >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END AS level_length_cume_dist_tier,
        service,
        attr_key,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE attr_key = 'version') AS version_rows,
        ROUND(AVG(level_length_cume_dist), 2) AS avg_level_length_cume_dist,
        COUNT(*) FILTER (WHERE service = 'auth') AS auth_rows
    FROM verified
    GROUP BY GROUPING SETS ((CASE WHEN level_length_cume_dist >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END, service, attr_key), (CASE WHEN level_length_cume_dist >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END, service))
)
SELECT
    level_length_cume_dist_tier,
    service,
    row_count,
    version_rows,
    avg_level_length_cume_dist
FROM stats
ORDER BY avg_level_length_cume_dist ASC;
