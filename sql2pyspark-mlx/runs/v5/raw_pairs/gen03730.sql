WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, v
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code DESC, id, key) AS latest_rank
    FROM exploded
    QUALIFY latest_rank = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM latest
    QUALIFY RANK() OVER (PARTITION BY level ORDER BY status_code) = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM ranked
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Grouped metrics
SELECT
    CASE WHEN island_id >= 9 THEN 'LARGE' ELSE 'SMALL' END AS island_id_tier,
    COUNT(DISTINCT key) AS distinct_keys,
    MIN(island_id) AS min_island_id,
    COUNT(*) FILTER (WHERE host = 'h1') AS h1_rows,
    PERCENTILE_APPROX(status_code, ARRAY(0.25, 0.5, 0.75)) AS status_code_quartiles,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    COUNT(*) FILTER (WHERE level = 'DEBUG') AS debug_rows
FROM sessions_app_logs
GROUP BY CUBE (CASE WHEN island_id >= 9 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY h1_rows DESC;
