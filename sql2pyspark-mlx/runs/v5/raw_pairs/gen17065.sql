WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
),
with_windows AS (
    SELECT
        *,
        AVG(latency_ms) OVER (PARTITION BY service ORDER BY latency_ms, id, attr_key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS latency_ms_avg_2,
        LAST_VALUE(attr_key) IGNORE NULLS OVER (PARTITION BY level ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_attr_key,
        LAST_VALUE(latency_ms, TRUE) OVER (PARTITION BY level, attr_key ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_latency_ms,
        ROW_NUMBER() OVER (PARTITION BY level, attr_key ORDER BY status_code DESC, id, attr_key) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY status_code DESC, id, attr_key) AS event_seq
    FROM lines
    WHERE level != 'DEBUG'
),
enriched AS (
    SELECT
        *,
        SUM(event_seq) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS event_seq_sum_2h,
        ROW_NUMBER() OVER (ORDER BY row_seq DESC, id, attr_key) AS row_seq_2,
        ROUND(last_known_latency_ms / SUM(last_known_latency_ms) OVER (PARTITION BY level), 4) AS last_known_latency_ms_share
    FROM with_windows
),
metrics AS (
    SELECT
        *,
        SUM(last_known_latency_ms_share) OVER (PARTITION BY level ORDER BY event_seq_sum_2h, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_last_known_latency_ms_share,
        COUNT(last_known_latency_ms_share) OVER (PARTITION BY level ORDER BY event_seq_sum_2h, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_last_known_latency_ms_share,
        AVG(last_known_latency_ms_share) OVER (PARTITION BY level ORDER BY event_seq_sum_2h, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_last_known_latency_ms_share
    FROM enriched
),
with_windows_app_logs AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        attr_key,
        attr_value,
        latency_ms_avg_2,
        event_seq,
        row_seq_2,
        last_known_latency_ms_share,
        running_sum_last_known_latency_ms_share,
        running_avg_last_known_latency_ms_share,
        FIRST_VALUE(running_avg_last_known_latency_ms_share) IGNORE NULLS OVER (PARTITION BY level ORDER BY row_seq_2, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_avg_last_known_latency_ms_share,
        AVG(last_known_latency_ms_share) OVER (PARTITION BY service, attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS last_known_latency_ms_share_avg_2h,
        CUME_DIST() OVER w_running AS row_seq_2_cume_dist,
        ROW_NUMBER() OVER (ORDER BY status_code DESC, id, attr_key) AS seq_no
    FROM metrics
    WINDOW w_running AS (ORDER BY row_seq_2 NULLS FIRST)
)
SELECT
    CASE WHEN running_sum_last_known_latency_ms_share >= 3.1 THEN 'HIGH' ELSE 'LOW' END AS running_sum_last_known_latency_ms_share_tier,
    SUM(CASE WHEN service = 'search' THEN last_known_latency_ms_share ELSE 0 END) AS search_last_known_latency_ms_share,
    PERCENTILE_APPROX(row_seq_2, 0.5) AS row_seq_2_p50,
    BIT_OR(latency_ms) AS latency_ms_bit_or,
    SUM(CASE WHEN level = 'DEBUG' THEN last_known_latency_ms_share_avg_2h ELSE 0 END) AS debug_last_known_latency_ms_share_avg_2h,
    COUNT_IF(level = 'DEBUG') AS debug_events
FROM with_windows_app_logs
GROUP BY CASE WHEN running_sum_last_known_latency_ms_share >= 3.1 THEN 'HIGH' ELSE 'LOW' END
