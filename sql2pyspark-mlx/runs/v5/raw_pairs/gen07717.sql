WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, attr_value
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM lines
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        SUM(session_start_flag) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        session_no,
        latency_ms - session_no AS latency_ms_minus_session_no,
        CASE WHEN host IN ('h1', 'h4') THEN 'Group A' ELSE 'Other' END AS host_group,
        COALESCE(k, 'DIRECT') AS k_filled
    FROM sessionized
    WHERE level = 'DEBUG'
      AND log_ts > TIMESTAMP '2025-06-01 06:00:00'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        level,
        latency_ms,
        k,
        latency_ms_minus_session_no
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY log_ts DESC, id, k) AS latest_rank
        FROM cleaned
    ) ranked
    WHERE latest_rank = 1
),
cleaned_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        level,
        latency_ms,
        k,
        latency_ms_minus_session_no,
        SUBSTRING(level, 1, 2) AS level_prefix,
        CEIL(latency_ms) AS latency_ms_whole,
        COALESCE(k, 'NONE') AS resolved_k
    FROM deduped
    WHERE k IS NOT NULL
      AND level != 'WARN'
)
-- Aggregate with conditional metrics
SELECT
    k,
    CASE WHEN latency_ms_whole >= 66 THEN 'LARGE' ELSE 'SMALL' END AS latency_ms_whole_tier,
    ROUND(SUM(latency_ms_minus_session_no) / NULLIF(COUNT(CASE WHEN level = 'INFO' THEN 1 END), 0), 2) AS latency_ms_minus_session_no_per_info,
    BIT_AND(latency_ms) AS latency_ms_bit_and,
    RANK() OVER (PARTITION BY k ORDER BY BIT_AND(latency_ms) DESC) AS latency_ms_bit_and_rank
FROM cleaned_app_logs
GROUP BY k, CASE WHEN latency_ms_whole >= 66 THEN 'LARGE' ELSE 'SMALL' END
HAVING ROUND(SUM(latency_ms_minus_session_no) / NULLIF(COUNT(CASE WHEN level = 'INFO' THEN 1 END), 0), 2) > 0
ORDER BY latency_ms_bit_and;
