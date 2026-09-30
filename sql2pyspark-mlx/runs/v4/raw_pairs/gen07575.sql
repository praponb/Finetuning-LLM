WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        host,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2
    FROM exploded
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER w_part status_code_high_cume_dist,
        MAX(status_code_high) OVER w_recent host_max_status_code_high,
        SUM(flag_bit2) OVER w_recent host_sum_flag_bit2
    FROM parsed
    WINDOW w_part AS (PARTITION BY host ORDER BY status_code_high NULLS FIRST), w_recent AS (PARTITION BY host)
),
base AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(host)) host_title
    FROM enriched
    WHERE status_code_high_cume_dist <= 0.74
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        host,
        status_code_high,
        flag_bit2,
        status_code_high_cume_dist,
        host_max_status_code_high,
        host_sum_flag_bit2,
        TRIM(host) clean_host,
        host_max_status_code_high / NULLIF(status_code_high, 0) host_max_status_code_high_per_status_code_high
    FROM base
    WHERE host_max_status_code_high IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    host,
    status_code_high,
    flag_bit2,
    status_code_high_cume_dist,
    host_max_status_code_high,
    host_sum_flag_bit2,
    clean_host,
    host_max_status_code_high_per_status_code_high,
    PERCENT_RANK() OVER (ORDER BY host_max_status_code_high_per_status_code_high NULLS FIRST) host_max_status_code_high_per_status_code_high_percent_rank,
    STDDEV_SAMP(status_code_high_cume_dist) OVER (PARTITION BY host) host_stddev_samp_status_code_high_cume_dist,
    RANK() OVER (PARTITION BY host ORDER BY host_max_status_code_high NULLS FIRST) host_max_status_code_high_rank,
    DENSE_RANK() OVER (PARTITION BY host ORDER BY flag_bit2 DESC NULLS FIRST) flag_bit2_dense_rank,
    DENSE_RANK() OVER (PARTITION BY host ORDER BY status_code_high DESC) status_code_high_dense_rank
FROM cleaned
