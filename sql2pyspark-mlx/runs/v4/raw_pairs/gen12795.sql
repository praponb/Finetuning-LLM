WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, v
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        host,
        log_ts,
        status_code,
        k,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS has_fault
    FROM flattened
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        log_ts,
        status_code,
        k,
        status_code_high,
        has_fault - status_code_high AS has_fault_minus_status_code_high,
        UPPER(k) AS k_upper
    FROM parsed
    WHERE log_ts >= TIMESTAMP '2025-06-01 09:00:00'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) + INTERVAL 15 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY is_new_session, id, k) AS row_num
        FROM islands
    ) ranked
    WHERE row_num = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(status_code, 1) AS status_code_rounded
    FROM ranked
    WHERE log_ts > DATE '2025-06-01'
      AND k IS NOT NULL
),
ranked_app_logs AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY row_num DESC, id, k) AS row_num_2
        FROM filtered
    ) t
    WHERE row_num_2 = 1
),
daily AS (
    -- Multi-level aggregation
    SELECT
        session_id,
        COUNT(CASE WHEN k = 'version' THEN 1 END) AS version_count,
        MIN(log_ts) AS started_at,
        ROUND(AVG(has_fault_minus_status_code_high), 2) AS avg_has_fault_minus_status_code_high
    FROM ranked_app_logs
    GROUP BY session_id
)
SELECT
    session_id,
    version_count,
    started_at,
    avg_has_fault_minus_status_code_high
FROM daily
ORDER BY version_count DESC NULLS LAST;
