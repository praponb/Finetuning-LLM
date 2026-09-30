WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, attr_value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(service, 1, 3) AS service_prefix,
        COALESCE(key, 'DIRECT') AS resolved_key
    FROM exploded
    WHERE status_code > 325
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(status_code) OVER w_grp AS service_sum_status_code,
        RANK() OVER (ORDER BY status_code) AS status_code_rank,
        COUNT(status_code) OVER (PARTITION BY key) AS key_count_status_code,
        STDDEV_SAMP(status_code) OVER (PARTITION BY host) AS host_stddev_samp_status_code,
        DENSE_RANK() OVER (PARTITION BY key ORDER BY status_code) AS status_code_dense_rank
    FROM filtered
    WINDOW w_grp AS (PARTITION BY service)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(key, '/', host) AS key_host,
        COALESCE(key, 'N/A') AS key_filled,
        COALESCE(key, 'UNKNOWN') AS key_clean
    FROM with_windows
    WHERE key IS NOT NULL
      AND service_sum_status_code >= 1654
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY service_sum_status_code) AS service_sum_status_code_percent_rank,
        CUME_DIST() OVER (PARTITION BY service ORDER BY status_code DESC) AS status_code_cume_dist,
        RANK() OVER (ORDER BY service_sum_status_code NULLS LAST) AS service_sum_status_code_rank,
        CUME_DIST() OVER (ORDER BY status_code_rank NULLS FIRST) AS status_code_rank_cume_dist
    FROM valid
    WHERE host_stddev_samp_status_code >= 439
)
SELECT
    id,
    service,
    host,
    key,
    attr_value,
    service_prefix,
    resolved_key,
    key_filled,
    key_clean,
    CASE WHEN key RLIKE '^(version|trace)$' THEN 'OTHER_ZONE' WHEN key RLIKE '^(region)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS key_zone
FROM metrics
ORDER BY service;
