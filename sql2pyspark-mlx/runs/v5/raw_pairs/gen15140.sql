WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host, key), 4) AS status_code_share,
        COUNT(latency_ms) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_count_15m,
        MIN(latency_ms) OVER (PARTITION BY host) AS host_min_latency_ms,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY host), 4) AS status_code_share_2
    FROM exploded
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        log_ts,
        status_code,
        v,
        status_code_share,
        host_min_latency_ms,
        status_code_share_2,
        STDDEV_SAMP(status_code) OVER (PARTITION BY key) AS key_stddev_samp_status_code
    FROM enriched
),
metrics AS (
    -- Ranking and gap features
    SELECT
        host,
        log_ts,
        v,
        host_min_latency_ms,
        status_code_share_2,
        key_stddev_samp_status_code,
        AVG(key_stddev_samp_status_code) OVER (PARTITION BY host) AS host_avg_key_stddev_samp_status_code,
        RANK() OVER (PARTITION BY host ORDER BY host_min_latency_ms ASC NULLS LAST) AS host_min_latency_ms_rank
    FROM scored
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        status_code_share_2 - key_stddev_samp_status_code AS status_code_share_2_minus_key_stddev_samp_status_code,
        CAST(host_min_latency_ms_rank AS DOUBLE) AS host_min_latency_ms_rank_str
    FROM metrics
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 09:00:00')
)
SELECT
    host,
    status_code_share_2,
    key_stddev_samp_status_code,
    host_avg_key_stddev_samp_status_code,
    host_min_latency_ms_rank,
    status_code_share_2_minus_key_stddev_samp_status_code,
    host_min_latency_ms_rank_str
FROM valid;
