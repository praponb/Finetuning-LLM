WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        attr_key,
        attr_value,
        CONCAT_WS(',', level, service) AS level_service_key
    FROM flattened
    WHERE log_ts > DATE '2025-06-01'
      AND attr_key IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        host,
        status_code,
        attr_key,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key, service), 4) AS status_code_share,
        PERCENT_RANK() OVER (PARTITION BY host, service ORDER BY status_code DESC) AS status_code_percent_rank,
        SUM(status_code) OVER (PARTITION BY host) AS host_sum_status_code,
        COUNT(status_code) OVER (PARTITION BY level) AS level_count_status_code
    FROM filtered
    WHERE level IN ('WARN', 'DEBUG')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, status_code_share * status_code AS status_code_share_times_status_code
    FROM enriched
    WHERE host_sum_status_code > 1114
),
base_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(host) AS clean_host,
        status_code_share_times_status_code * host_sum_status_code AS status_code_share_times_status_code_times_host_sum_status_code
    FROM base
    WHERE attr_key IS DISTINCT FROM 'version'
      AND host = 'h2'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        host,
        status_code,
        host_sum_status_code,
        level_count_status_code,
        status_code_share_times_status_code,
        status_code_share_times_status_code_times_host_sum_status_code,
        ROUND(level_count_status_code * 1.15, 2) AS level_count_status_code_scaled,
        ROUND(status_code_share_times_status_code_times_host_sum_status_code / level_count_status_code, 2) AS status_code_share_times_status_code_times_host_sum_status_code_per_level_count_status_code,
        COALESCE(attr_key, 'N/A') AS resolved_attr_key
    FROM base_app_logs
    WHERE status_code_share_times_status_code_times_host_sum_status_code BETWEEN 33 AND 68
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN level_count_status_code_scaled >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END AS level_count_status_code_scaled_tier,
    host,
    CASE WHEN level_count_status_code >= 16 THEN 'HIGH' ELSE 'LOW' END AS level_count_status_code_tier,
    MAX(level_count_status_code) FILTER (WHERE host = 'h4') AS h4_max_level_count_status_code,
    SORT_ARRAY(COLLECT_SET(host)) AS host_list,
    COUNT(*) FILTER (WHERE host = 'h4') AS h4_rows,
    COUNT(CASE WHEN host = 'h3' THEN 1 END) AS h3_count,
    SIZE(COLLECT_SET(host)) AS num_hosts,
    ROUND(SUM(status_code) / NULLIF(COUNT(CASE WHEN host = 'h4' THEN 1 END), 0), 2) AS status_code_per_h4
FROM cleaned
GROUP BY CASE WHEN level_count_status_code_scaled >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END, host, CASE WHEN level_count_status_code >= 16 THEN 'HIGH' ELSE 'LOW' END
ORDER BY h3_count DESC NULLS FIRST;
