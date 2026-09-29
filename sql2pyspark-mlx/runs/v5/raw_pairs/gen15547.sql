WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM exploded
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY session_start_flag DESC, id, key) event_seq,
        ROW_NUMBER() OVER (PARTITION BY host, key ORDER BY session_start_flag, id, key) seq_no
    FROM sessionized
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        log_ts,
        key,
        session_start_flag,
        seq_no,
        LEAD(island_id) OVER w_ordered next_island_id,
        LAG(session_start_flag) OVER w_ordered prev_session_start_flag,
        LEAD(session_start_flag) OVER w_ordered next_session_start_flag,
        ROW_NUMBER() OVER w_ordered seq_no_2
    FROM scored
    WINDOW w_ordered AS (PARTITION BY host ORDER BY event_seq, id, key)
),
scored_app_logs AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        key,
        session_start_flag,
        seq_no_2,
        NTILE(4) OVER (PARTITION BY key ORDER BY seq_no DESC, id, key) seq_no_bucket,
        NTILE(5) OVER (ORDER BY session_start_flag, id, key) session_start_flag_bucket,
        FIRST_VALUE(prev_session_start_flag, TRUE) OVER (PARTITION BY key ORDER BY seq_no, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_prev_session_start_flag
    FROM metrics
)
SELECT
    id,
    key,
    session_start_flag,
    seq_no_2,
    seq_no_bucket,
    session_start_flag_bucket,
    first_known_prev_session_start_flag,
    CASE WHEN key RLIKE '^(region|version)$' THEN 'CORE' ELSE 'EXTENDED' END AS key_zone
FROM scored_app_logs
