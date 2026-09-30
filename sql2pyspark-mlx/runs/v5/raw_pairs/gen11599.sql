WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        log_ts,
        level,
        status_code,
        k,
        CAST((status_code & 4) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, k) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM parsed
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY level ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        level,
        status_code,
        k,
        status_code_high,
        burst_id,
        SUM(status_code) OVER w_running AS running_sum_status_code,
        MAX(status_code) OVER w_running AS running_max_status_code,
        AVG(status_code) OVER w_running AS running_avg_status_code
    FROM islands
    WINDOW w_running AS (PARTITION BY level ORDER BY burst_id, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY k, level ORDER BY status_code_high DESC, id, k) AS status_code_high_bucket,
        AVG(burst_id) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS burst_id_avg_30m,
        ROW_NUMBER() OVER (PARTITION BY level, k ORDER BY running_max_status_code DESC, id, k) AS event_seq,
        PERCENT_RANK() OVER w_part AS running_avg_status_code_percent_rank,
        CUME_DIST() OVER (PARTITION BY level, k ORDER BY running_sum_status_code) AS running_sum_status_code_cume_dist
    FROM enriched
    WHERE status_code_high <= 2
    WINDOW w_part AS (ORDER BY running_avg_status_code NULLS LAST)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(status_code) OVER w_recent AS level_count_status_code,
        SUM(status_code) OVER w_running AS running_status_code,
        LAG(running_sum_status_code, 1) OVER (ORDER BY burst_id_avg_30m, id, k) AS prev_running_sum_status_code,
        FIRST_VALUE(running_avg_status_code, TRUE) OVER (PARTITION BY k ORDER BY running_sum_status_code_cume_dist, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_avg_status_code
    FROM enriched_app_logs
    WINDOW w_recent AS (PARTITION BY level, k), w_running AS (PARTITION BY level ORDER BY event_seq, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    COUNT(*) FILTER (WHERE level = 'ERROR') AS error_rows,
    ROUND(AVG(event_seq), 2) AS avg_event_seq,
    COUNT(DISTINCT k) AS distinct_ks,
    COUNT(*) FILTER (WHERE k = 'region') AS region_rows
FROM metrics
GROUP BY DATE_TRUNC('HOUR', log_ts);
