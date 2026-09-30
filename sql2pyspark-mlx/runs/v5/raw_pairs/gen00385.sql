WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, value
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY latency_ms NULLS FIRST, id, key) AS rn
        FROM lines
    ) x
    WHERE rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        SUM(latency_ms) OVER w_recent AS cumulative_latency_ms,
        RANK() OVER (PARTITION BY level ORDER BY status_code DESC) AS status_code_rank,
        CUME_DIST() OVER (PARTITION BY host ORDER BY status_code DESC) AS status_code_cume_dist,
        LAG(latency_ms, 1) OVER (PARTITION BY host, service ORDER BY latency_ms, id, key) AS prev_latency_ms
    FROM latest
    WINDOW w_ordered AS (ORDER BY latency_ms DESC, id, key), w_recent AS (PARTITION BY level ORDER BY latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(seq_no, 0) AS seq_no_filled
    FROM with_windows
    WHERE key IS NOT NULL
      AND latency_ms BETWEEN 980 AND 2357
),
latest_app_logs AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        key,
        rn,
        seq_no,
        status_code_cume_dist,
        prev_latency_ms
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level, host ORDER BY latency_ms DESC NULLS LAST, id, key) AS rn_2
        FROM cleaned
    ) x
    WHERE rn_2 = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(key) AS key_lower
    FROM latest_app_logs
    WHERE rn <> 1
)
SELECT
    id,
    service,
    level,
    latency_ms,
    key,
    seq_no,
    status_code_cume_dist,
    CASE WHEN level RLIKE '^(INFO|ERROR)$' THEN 'EXTENDED' WHEN level RLIKE '^(WARN|DEBUG)$' THEN 'CORE' ELSE 'GROUP_A' END AS level_zone
FROM valid
