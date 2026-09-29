WITH lines AS (
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
    LATERAL VIEW EXPLODE(context) t AS k, attr_value
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts ASC, id ASC, k ASC) AS prev_log_ts
    FROM lines
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY host ORDER BY log_ts ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        k,
        attr_value,
        prev_log_ts,
        is_new_session,
        session_no,
        is_new_session - status_code AS is_new_session_minus_status_code,
        LEAST(status_code, is_new_session) AS least_status_code_is_new_session,
        COALESCE(k, 'N/A') AS resolved_k
    FROM islands
    WHERE k IS NOT NULL
      AND log_ts > TO_DATE('2025-06-01')
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts ASC, id ASC, k ASC) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts ASC, id ASC, k ASC) + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM base
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        host,
        log_ts,
        level,
        k,
        attr_value,
        prev_log_ts,
        session_no,
        least_status_code_is_new_session,
        resolved_k,
        SUM(is_new_burst) OVER (PARTITION BY host ORDER BY log_ts ASC, id ASC, k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps_2
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        level,
        k,
        attr_value,
        resolved_k,
        session_id,
        COUNT(least_status_code_is_new_session) OVER w_recent AS last7_count_least_status_code_is_new_session,
        MAX(least_status_code_is_new_session) OVER w_recent AS last7_max_least_status_code_is_new_session,
        SUM(least_status_code_is_new_session) OVER w_recent AS last7_sum_least_status_code_is_new_session,
        NTILE(3) OVER (PARTITION BY k ORDER BY session_no DESC, id ASC, k ASC) AS session_no_bucket
    FROM sessions_app_logs
    WINDOW w_recent AS (PARTITION BY host, k ORDER BY log_ts ASC, id ASC, k ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN last7_max_least_status_code_is_new_session >= 86 THEN 'PREMIUM' ELSE 'STANDARD' END AS last7_max_least_status_code_is_new_session_tier,
    CASE WHEN last7_sum_least_status_code_is_new_session >= 430 THEN 'LARGE' ELSE 'SMALL' END AS last7_sum_least_status_code_is_new_session_tier,
    SIZE(COLLECT_SET(level)) AS num_levels,
    COUNT(*) FILTER (WHERE k = 'version') AS version_rows,
    COUNT(*) FILTER (WHERE level = 'ERROR') AS error_rows,
    COUNT(*) FILTER (WHERE host = 'h1') AS h1_rows
FROM enriched
GROUP BY CUBE (CASE WHEN last7_max_least_status_code_is_new_session >= 86 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last7_sum_least_status_code_is_new_session >= 430 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY version_rows DESC;
