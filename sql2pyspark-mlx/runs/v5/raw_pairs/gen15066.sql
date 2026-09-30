WITH exploded AS (
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
    LATERAL VIEW OUTER EXPLODE(context) t AS k, attr_value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        status_code,
        k,
        attr_value,
        DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts,
        COALESCE(k, 'UNKNOWN') AS resolved_k
    FROM exploded
    WHERE k NOT IN ('region', 'version')
),
filtered_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(host)) AS host_title,
        COALESCE(k, 'DIRECT') AS resolved_k_2
    FROM filtered
    WHERE k IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        service,
        host,
        status_code,
        k,
        days_since_log_ts,
        resolved_k_2,
        STDDEV_SAMP(days_since_log_ts) OVER (PARTITION BY host) AS host_stddev_samp_days_since_log_ts,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY days_since_log_ts ASC NULLS LAST) AS days_since_log_ts_dense_rank,
        PERCENT_RANK() OVER (ORDER BY days_since_log_ts ASC) AS days_since_log_ts_percent_rank
    FROM filtered_app_logs
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(days_since_log_ts AS STRING) AS days_since_log_ts_str,
        CASE WHEN host IN ('h2', 'h3') THEN 'Core' ELSE 'Other' END AS host_group,
        COALESCE(k, 'N/A') AS resolved_k
    FROM scored
    WHERE host = 'h3'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(days_since_log_ts_dense_rank) OVER (PARTITION BY host) AS host_count_days_since_log_ts_dense_rank,
        AVG(days_since_log_ts_percent_rank) OVER (PARTITION BY host) AS host_avg_days_since_log_ts_percent_rank,
        CUME_DIST() OVER (ORDER BY days_since_log_ts DESC) AS days_since_log_ts_cume_dist
    FROM base
)
SELECT
    service,
    k,
    days_since_log_ts,
    days_since_log_ts_percent_rank,
    days_since_log_ts_str,
    host_group,
    host_count_days_since_log_ts_dense_rank,
    host_avg_days_since_log_ts_percent_rank,
    days_since_log_ts_cume_dist,
    CASE WHEN service RLIKE '^(billing|search)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS service_zone
FROM enriched
ORDER BY service_zone DESC NULLS LAST
