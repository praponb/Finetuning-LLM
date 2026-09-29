WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(latency_ms) OVER w_part AS next_latency_ms,
        LAG(latency_ms) OVER w_part AS prev_latency_ms,
        LAG(status_code) OVER w_part AS prev_status_code,
        LEAD(status_code) OVER w_part AS next_status_code,
        ROW_NUMBER() OVER w_part AS seq_no,
        LAG(latency_ms, 1) OVER w_running AS prev_latency_ms_2
    FROM exploded
    WHERE status_code = 431
    WINDOW w_part AS (PARTITION BY level ORDER BY status_code, id, attr_key), w_running AS (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        latency_ms,
        status_code,
        attr_key,
        attr_value,
        next_latency_ms,
        prev_latency_ms_2,
        SUM(prev_latency_ms) OVER w_recent AS running_sum_prev_latency_ms,
        AVG(prev_latency_ms) OVER w_recent AS running_avg_prev_latency_ms,
        COUNT(prev_latency_ms) OVER w_recent AS running_count_prev_latency_ms,
        CUME_DIST() OVER (PARTITION BY level ORDER BY latency_ms DESC) AS latency_ms_cume_dist
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY attr_key ORDER BY next_latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY running_count_prev_latency_ms, id, attr_key) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    latency_ms,
    status_code,
    attr_key,
    prev_latency_ms_2,
    running_avg_prev_latency_ms,
    running_count_prev_latency_ms,
    latency_ms_cume_dist,
    rank_in_group,
    CASE WHEN attr_key RLIKE '^(trace|region)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS attr_key_zone
FROM latest
ORDER BY latency_ms_cume_dist DESC NULLS LAST, running_count_prev_latency_ms DESC;
