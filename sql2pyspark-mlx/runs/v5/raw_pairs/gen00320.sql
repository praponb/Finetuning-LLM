WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, value
),
parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        log_ts,
        status_code,
        attr_key,
        value,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY attr_key ORDER BY status_code_high, id, attr_key) status_code_high_bucket,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) status_code_share,
        SUM(status_code) OVER (PARTITION BY attr_key ORDER BY status_code_high, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_status_code,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY status_code DESC, id, attr_key) seq_no
    FROM parsed
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key) AS prev_log_ts
    FROM scored
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY attr_key ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
scored_app_logs AS (
    -- Ranking and gap features
    SELECT
        id,
        log_ts,
        status_code,
        attr_key,
        is_flagged,
        status_code_high,
        status_code_high_bucket,
        status_code_share,
        running_status_code,
        seq_no,
        prev_log_ts,
        is_new_burst,
        burst_id,
        SUM(status_code) OVER (PARTITION BY attr_key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) status_code_sum_15m,
        STDDEV_SAMP(is_flagged) OVER (PARTITION BY attr_key) attr_key_stddev_samp_is_flagged
    FROM sessionized
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN status_code_sum_15m >= 1799 THEN 'HIGH' ELSE 'LOW' END AS status_code_sum_15m_tier,
        seq_no,
        SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
        COUNT(*) AS cnt,
        PERCENTILE_APPROX(is_flagged, ARRAY(0.25, 0.5, 0.75)) AS is_flagged_quartiles,
        COUNT(CASE WHEN attr_key = 'version' THEN 1 END) AS version_count,
        COUNT_IF(attr_key = 'version') AS version_events,
        PERCENTILE_APPROX(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles
    FROM scored_app_logs
    GROUP BY CASE WHEN status_code_sum_15m >= 1799 THEN 'HIGH' ELSE 'LOW' END, seq_no
    HAVING version_events >= 1
)
SELECT
    status_code_sum_15m_tier,
    seq_no,
    attr_key_list,
    cnt,
    is_flagged_quartiles
FROM totals
ORDER BY seq_no
