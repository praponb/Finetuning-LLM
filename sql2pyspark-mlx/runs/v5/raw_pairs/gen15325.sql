WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, k) AS prev_log_ts
    FROM exploded
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        service,
        log_ts,
        k,
        attr_value,
        SUM(is_new_session) OVER (PARTITION BY service ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        log_ts,
        k,
        attr_value,
        session_id,
        FIRST_VALUE(k, TRUE) OVER (PARTITION BY service ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k,
        SUM(session_id) OVER (PARTITION BY k) AS k_sum_session_id,
        NTILE(4) OVER (PARTITION BY service ORDER BY session_id DESC, id, k) AS session_id_bucket,
        MAX(session_id) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS session_id_max_2h
    FROM sessionized
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY k ORDER BY session_id_max_2h) AS latest_rank
        FROM scored
    ) t
    WHERE latest_rank = 1
)
-- Running and moving aggregates
SELECT
    id,
    k,
    attr_value,
    first_known_k,
    session_id_max_2h,
    latest_rank,
    MAX(session_id_bucket) OVER (PARTITION BY k ORDER BY session_id_bucket, id, k ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_session_id_bucket,
    SUM(session_id_bucket) OVER (PARTITION BY k ORDER BY session_id_bucket, id, k ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_session_id_bucket,
    MIN(session_id_bucket) OVER (PARTITION BY k ORDER BY session_id_bucket, id, k ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_session_id_bucket,
    AVG(session_id_bucket) OVER (PARTITION BY k ORDER BY session_id_bucket, id, k ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_session_id_bucket,
    FIRST_VALUE(session_id_max_2h, TRUE) OVER (PARTITION BY service ORDER BY session_id, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_session_id_max_2h
FROM deduped
