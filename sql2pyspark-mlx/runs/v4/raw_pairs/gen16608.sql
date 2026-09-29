WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, v
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM flattened
    QUALIFY ROW_NUMBER() OVER (PARTITION BY service ORDER BY status_code, id, key) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CORR(latency_ms, status_code) OVER (PARTITION BY key) AS corr_latency_ms_status_code,
        CORR(latency_ms, status_code) OVER w_recent AS corr_latency_ms_status_code_2,
        AVG(latency_ms) OVER (PARTITION BY key ORDER BY status_code, id, key ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS latency_ms_avg_6_prior
    FROM ranked
    WINDOW w_recent AS (PARTITION BY service)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, service
    FROM metrics
    INTERSECT ALL
    SELECT id, service
    FROM app_logs
    WHERE host = 'h4'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.id = k.id AND s.service = k.service
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        key,
        v,
        corr_latency_ms_status_code,
        corr_latency_ms_status_code_2,
        latency_ms_avg_6_prior,
        AVG(corr_latency_ms_status_code_2) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS corr_latency_ms_status_code_2_avg_30m,
        CORR(latency_ms, status_code) OVER (PARTITION BY service) AS corr_latency_ms_status_code_3,
        PERCENT_RANK() OVER (PARTITION BY key, service ORDER BY status_code DESC) AS status_code_percent_rank
    FROM verified
)
-- Grouped metrics
SELECT
    key,
    CASE WHEN corr_latency_ms_status_code_2_avg_30m >= 0 THEN 'LARGE' ELSE 'SMALL' END AS corr_latency_ms_status_code_2_avg_30m_tier,
    STDDEV_SAMP(latency_ms) AS latency_ms_stddev,
    COUNT(*) AS event_count,
    ROUND(AVG(corr_latency_ms_status_code_2_avg_30m), 2) AS avg_corr_latency_ms_status_code_2_avg_30m
FROM enriched
GROUP BY key, CASE WHEN corr_latency_ms_status_code_2_avg_30m >= 0 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY latency_ms_stddev NULLS FIRST;
