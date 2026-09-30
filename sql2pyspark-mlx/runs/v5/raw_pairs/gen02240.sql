WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, v
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM exploded
    QUALIFY ROW_NUMBER() OVER (PARTITION BY service, level ORDER BY latency_ms DESC NULLS FIRST, id, attr_key) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        status_code,
        attr_key,
        v,
        CORR(status_code, latency_ms) OVER (PARTITION BY service) AS corr_status_code_latency_ms,
        SUM(status_code) OVER (PARTITION BY service ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code,
        STDDEV_SAMP(latency_ms) OVER (PARTITION BY level, service) AS level_stddev_samp_latency_ms,
        LAG(status_code, 1) OVER (ORDER BY status_code, id, attr_key) AS prev_status_code,
        RANK() OVER (PARTITION BY level ORDER BY status_code DESC) AS status_code_rank
    FROM ranked
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(running_status_code) OVER (PARTITION BY attr_key ORDER BY level_stddev_samp_latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_running_status_code,
        COUNT(running_status_code) OVER (PARTITION BY attr_key ORDER BY level_stddev_samp_latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_status_code,
        SUM(running_status_code) OVER (PARTITION BY attr_key ORDER BY level_stddev_samp_latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_status_code,
        SUM(status_code_rank) OVER (PARTITION BY attr_key ORDER BY prev_status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code_rank
    FROM with_windows
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(running_avg_running_status_code, 0) AS running_avg_running_status_code_filled
    FROM enriched
    WHERE running_sum_running_status_code IS NULL
)
-- Grouped metrics
SELECT
    attr_key,
    CASE WHEN running_sum_running_status_code >= 10500 THEN 'HIGH' ELSE 'LOW' END AS running_sum_running_status_code_tier,
    MAX(level_stddev_samp_latency_ms) AS max_level_stddev_samp_latency_ms,
    APPROX_PERCENTILE(level_stddev_samp_latency_ms, 0.95) AS level_stddev_samp_latency_ms_p95,
    COUNT(DISTINCT attr_key) AS distinct_attr_keys,
    COUNT_IF(attr_key = 'trace') AS trace_events,
    SUM(running_avg_running_status_code) FILTER (WHERE attr_key = 'version') AS version_total_running_avg_running_status_code
FROM cleaned
GROUP BY attr_key, CASE WHEN running_sum_running_status_code >= 10500 THEN 'HIGH' ELSE 'LOW' END
HAVING distinct_attr_keys < 1000;
