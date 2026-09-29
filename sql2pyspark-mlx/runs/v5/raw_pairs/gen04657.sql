WITH exploded AS (
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
    LATERAL VIEW EXPLODE(context) x AS attr_key, attr_value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        latency_ms,
        status_code,
        attr_key,
        CAST((status_code & 4) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE attr_key IN ('trace', 'version', 'region')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        latency_ms,
        status_code,
        attr_key,
        COUNT(latency_ms) OVER (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_latency_ms,
        SUM(latency_ms) OVER (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_latency_ms,
        MAX(latency_ms) OVER (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_latency_ms,
        MIN(latency_ms) OVER (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_latency_ms,
        CORR(latency_ms, status_code) OVER (PARTITION BY attr_key) corr_latency_ms_status_code
    FROM parsed
),
scored_app_logs AS (
    -- Ranking and gap features
    SELECT
        id,
        attr_key,
        running_count_latency_ms,
        running_sum_latency_ms,
        corr_latency_ms_status_code,
        MIN(status_code) OVER (PARTITION BY attr_key) attr_key_min_status_code,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY running_sum_latency_ms NULLS LAST) running_sum_latency_ms_cume_dist,
        COUNT(latency_ms) OVER (PARTITION BY attr_key) attr_key_count_latency_ms,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY running_min_latency_ms DESC NULLS LAST) running_min_latency_ms_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY running_max_latency_ms, id, attr_key) event_seq
    FROM scored
    WHERE running_max_latency_ms BETWEEN 1700 AND 2300
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        running_sum_latency_ms + running_count_latency_ms running_sum_latency_ms_plus_running_count_latency_ms,
        COALESCE(attr_key, 'OTHER') attr_key_clean
    FROM scored_app_logs
    WHERE corr_latency_ms_status_code <= -0.4
),
valid_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(running_sum_latency_ms) running_sum_latency_ms_whole,
        COALESCE(attr_key, 'N/A') attr_key_filled
    FROM valid
    WHERE attr_key_min_status_code >= 359
)
-- Multi-level aggregation
SELECT
    attr_key,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys,
    ROUND(SUM(running_sum_latency_ms_plus_running_count_latency_ms) / NULLIF(COUNT(CASE WHEN attr_key = 'trace' THEN 1 END), 0), 2) AS running_sum_latency_ms_plus_running_count_latency_ms_per_trace
FROM valid_app_logs
GROUP BY attr_key
ORDER BY num_attr_keys DESC
