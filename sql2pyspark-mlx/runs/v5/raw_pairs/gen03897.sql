WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, attr_value
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM exploded
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(is_new_session, 0) AS is_new_session_filled,
        COALESCE(k, 'N/A') AS k_filled
    FROM sessionized
    WHERE is_new_session != 0
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        level,
        k,
        attr_value,
        k_filled,
        SUM(is_new_session) OVER w_part AS last7_sum_is_new_session,
        COUNT(is_new_session) OVER w_part AS last7_count_is_new_session,
        AVG(is_new_session) OVER w_part AS last7_avg_is_new_session,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY k), 4) AS latency_ms_share
    FROM base
    WINDOW w_part AS (PARTITION BY k, level ORDER BY session_no, id, k ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        log_ts,
        level,
        k,
        attr_value,
        k_filled,
        last7_sum_is_new_session,
        last7_count_is_new_session,
        last7_avg_is_new_session,
        latency_ms_share,
        HOUR(log_ts) AS log_ts_hour
    FROM metrics
    WHERE log_ts >= '2023-08-01'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY log_ts_hour DESC NULLS LAST, id, k) AS rn
        FROM filtered
    ) x
    WHERE rn = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        level,
        k,
        k_filled,
        last7_count_is_new_session,
        latency_ms_share,
        log_ts_hour,
        rn,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY last7_count_is_new_session NULLS FIRST) AS last7_count_is_new_session_dense_rank,
        COUNT(log_ts_hour) OVER (PARTITION BY k, level ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS log_ts_hour_count_15m,
        LAST_VALUE(latency_ms_share) IGNORE NULLS OVER (ORDER BY latency_ms_share, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_latency_ms_share
    FROM deduped
    WHERE last7_avg_is_new_session BETWEEN 0.22 AND 0.58
)
SELECT
    id,
    level,
    k,
    last7_count_is_new_session,
    log_ts_hour,
    rn,
    last7_count_is_new_session_dense_rank,
    log_ts_hour_count_15m,
    last_known_latency_ms_share,
    CASE WHEN level RLIKE '^(DEBUG|WARN)$' THEN 'GROUP_A' ELSE 'CORE' END AS level_zone
FROM enriched
ORDER BY id DESC, last7_count_is_new_session;
