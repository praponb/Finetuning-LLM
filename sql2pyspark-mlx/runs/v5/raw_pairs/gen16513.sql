WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS k, v
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, k) + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM exploded
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY level ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY k ORDER BY log_ts) AS rank_in_group
        FROM islands
    ) ranked
    WHERE rank_in_group = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, k) + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM ranked
),
islands_app_logs AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        log_ts,
        level,
        k,
        new_group_flag,
        island_id,
        rank_in_group,
        SUM(is_new_session) OVER (PARTITION BY level ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
ranked_app_logs AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY new_group_flag, id, k) AS rn
        FROM islands_app_logs
    ) ranked
    WHERE rn = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM ranked_app_logs
    QUALIFY RANK() OVER (PARTITION BY level, k ORDER BY rank_in_group) = 1
)
-- Ranking and gap features
SELECT
    id,
    log_ts,
    k,
    new_group_flag,
    island_id,
    rank_in_group,
    rn,
    FIRST_VALUE(k, TRUE) OVER (PARTITION BY k ORDER BY new_group_flag, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k,
    AVG(session_no) OVER (PARTITION BY level, k ORDER BY rank_in_group, id, k ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS session_no_avg_3_prior
FROM latest
