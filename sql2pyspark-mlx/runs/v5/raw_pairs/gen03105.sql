WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
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
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(host) AS host_length,
        INITCAP(TRIM(level)) AS level_title,
        COALESCE(attr_key, 'UNKNOWN') AS resolved_attr_key
    FROM exploded
    WHERE log_ts >= TIMESTAMP '2025-06-01 13:00:00'
      AND status_code <= 386
),
base_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        level,
        status_code,
        attr_key,
        value,
        host_length,
        level_title,
        resolved_attr_key,
        DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts
    FROM base
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 12:00:00')
      AND level NOT IN ('INFO', 'WARN', 'ERROR')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(host) AS host_length_2
    FROM base_app_logs
    WHERE attr_key IS NOT NULL
      AND attr_key IS DISTINCT FROM 'version'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(host) AS host_lower,
        COALESCE(attr_key, 'UNKNOWN') AS resolved_attr_key_2
    FROM filtered
    WHERE days_since_log_ts < 42
),
filtered_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LEAST(status_code, host_length) AS least_status_code_host_length
    FROM cleaned
    WHERE status_code = 350
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        host,
        status_code,
        value,
        level_title,
        resolved_attr_key,
        host_length_2,
        host_lower,
        least_status_code_host_length,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY status_code) AS status_code_dense_rank,
        COUNT(status_code) OVER w_hist AS level_count_status_code,
        STDDEV_SAMP(host_length) OVER (PARTITION BY host) AS host_stddev_samp_host_length
    FROM filtered_app_logs
    WHERE status_code BETWEEN 369 AND 430
    WINDOW w_hist AS (PARTITION BY level)
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN least_status_code_host_length >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS least_status_code_host_length_tier,
        host,
        SUM(CASE WHEN host = 'h2' THEN status_code ELSE 0 END) AS h2_status_code,
        AVG(host_stddev_samp_host_length) AS avg_host_stddev_samp_host_length,
        COUNT(*) FILTER (WHERE host = 'h1') AS h1_rows
    FROM enriched
    GROUP BY CUBE (CASE WHEN least_status_code_host_length >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END, host)
)
SELECT avg_host_stddev_samp_host_length, h1_rows
FROM stats
ORDER BY avg_host_stddev_samp_host_length NULLS LAST
