WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, value
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host, level), 4) AS status_code_share,
        COUNT(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_count_2h
    FROM lines
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        host,
        log_ts,
        level,
        k,
        value,
        status_code_share,
        UPPER(k) AS k_upper,
        CONCAT(service, ', ', level) AS service_level
    FROM enriched
    WHERE log_ts > DATE '2025-06-01'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        host,
        log_ts,
        level,
        value,
        status_code_share,
        k_upper,
        service_level,
        COALESCE(status_code_share, 0) AS status_code_share_filled
    FROM valid
    WHERE k IS NOT NULL
)
-- Running and moving aggregates
SELECT
    *,
    DENSE_RANK() OVER (PARTITION BY host ORDER BY status_code_share DESC NULLS LAST) AS status_code_share_dense_rank,
    STDDEV_SAMP(status_code_share_filled) OVER (PARTITION BY host, level) AS host_stddev_samp_status_code_share_filled,
    RANK() OVER (PARTITION BY host, level ORDER BY status_code_share_filled NULLS FIRST) AS status_code_share_filled_rank
FROM cleaned
