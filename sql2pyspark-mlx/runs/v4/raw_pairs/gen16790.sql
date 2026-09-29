WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value,
        CORR(status_code, latency_ms) OVER (PARTITION BY attr_key, service) AS corr_status_code_latency_ms,
        PERCENT_RANK() OVER (ORDER BY status_code DESC) AS status_code_percent_rank,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share_2
    FROM lines
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value,
        corr_status_code_latency_ms,
        status_code_percent_rank,
        status_code_share,
        status_code_share_2,
        CORR(status_code, latency_ms) OVER (PARTITION BY service, level) AS corr_status_code_latency_ms_2,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY status_code_percent_rank NULLS LAST) AS status_code_percent_rank_cume_dist
    FROM metrics
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CORR(status_code, latency_ms) OVER (PARTITION BY attr_key) AS corr_status_code_latency_ms_3,
        STDDEV_SAMP(status_code_percent_rank_cume_dist) OVER (PARTITION BY attr_key) AS attr_key_stddev_samp_status_code_percent_rank_cume_dist
    FROM scored
)
-- Multi-level aggregation
SELECT
    level,
    CASE WHEN status_code >= 441 THEN 'HIGH' ELSE 'LOW' END AS status_code_tier,
    SUM(status_code_percent_rank) FILTER (WHERE attr_key = 'trace') AS trace_total_status_code_percent_rank,
    COUNT(DISTINCT service) AS distinct_services,
    COUNT(CASE WHEN service = 'checkout' THEN 1 END) AS checkout_count,
    COUNT(*) FILTER (WHERE attr_key = 'trace') AS trace_rows,
    COUNT(CASE WHEN attr_key = 'region' THEN 1 END) AS region_count,
    ROUND(AVG(status_code), 2) AS avg_status_code
FROM enriched
GROUP BY level, CASE WHEN status_code >= 441 THEN 'HIGH' ELSE 'LOW' END
