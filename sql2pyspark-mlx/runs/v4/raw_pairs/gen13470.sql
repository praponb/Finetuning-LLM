WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, attr_value
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, attr_key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, attr_key) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM exploded
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY level ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
kept_keys AS (
    -- Set difference on keys
    SELECT level, id
    FROM islands
    EXCEPT
    SELECT level, id
    FROM app_logs
    WHERE log_ts >= '2023-06-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    INNER JOIN kept_keys k ON s.level = k.level AND s.id = k.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        host,
        level,
        attr_key,
        attr_value,
        session_start_flag,
        island_id,
        status_code - session_start_flag status_code_minus_session_start_flag,
        COALESCE(attr_key, 'OTHER') attr_key_filled
    FROM reconciled
    WHERE attr_key IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN status_code_minus_session_start_flag >= 89 THEN 'High' WHEN status_code_minus_session_start_flag >= 18 THEN 'Medium' ELSE 'Low' END status_code_minus_session_start_flag_level
    FROM valid
    WHERE session_start_flag = 1
),
valid_app_logs AS (
    -- Basic cleansing and defaults
    SELECT
        host,
        level,
        attr_key,
        attr_value,
        island_id,
        status_code_minus_session_start_flag,
        status_code_minus_session_start_flag_level,
        CONCAT(attr_key, ' - ', host) attr_key_host
    FROM cleaned
    WHERE session_start_flag <= 0
),
stats AS (
    -- Multi-level aggregation
    SELECT
        host,
        CASE WHEN island_id >= 6 THEN 'HIGH' ELSE 'LOW' END AS island_id_tier,
        COUNT(CASE WHEN host = 'h4' THEN 1 END) AS h4_count,
        MAX(island_id) AS max_island_id,
        ROUND(AVG(status_code_minus_session_start_flag), 2) AS avg_status_code_minus_session_start_flag,
        SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list
    FROM valid_app_logs
    GROUP BY ROLLUP (host, CASE WHEN island_id >= 6 THEN 'HIGH' ELSE 'LOW' END)
    HAVING h4_count < 1
)
SELECT
    host,
    island_id_tier,
    max_island_id,
    avg_status_code_minus_session_start_flag,
    attr_key_list
FROM stats
ORDER BY max_island_id DESC NULLS FIRST, island_id_tier DESC;
