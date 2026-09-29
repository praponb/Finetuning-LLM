WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        level,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, value
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM lines
    EXCEPT DISTINCT
    SELECT id
    FROM app_logs
    WHERE TO_DATE(log_ts) >= '2023-11-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN kept_keys k ON s.id = k.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        host,
        level,
        status_code,
        attr_key,
        value,
        MAX(status_code) OVER (PARTITION BY attr_key, host) AS attr_key_max_status_code,
        DENSE_RANK() OVER (ORDER BY status_code) AS status_code_dense_rank,
        CUME_DIST() OVER (PARTITION BY host ORDER BY status_code) AS status_code_cume_dist
    FROM reconciled
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(status_code_cume_dist) OVER (PARTITION BY attr_key) AS attr_key_sum_status_code_cume_dist,
        DENSE_RANK() OVER (ORDER BY status_code) AS status_code_dense_rank_2,
        PERCENT_RANK() OVER (ORDER BY attr_key_max_status_code DESC) AS attr_key_max_status_code_percent_rank,
        AVG(attr_key_max_status_code) OVER (PARTITION BY attr_key, level) AS attr_key_avg_attr_key_max_status_code
    FROM metrics
)
-- Grouped metrics
SELECT
    CASE WHEN status_code_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS status_code_dense_rank_tier,
    level,
    COUNT_IF(level = 'DEBUG') AS debug_events,
    MAX(attr_key_max_status_code) AS max_attr_key_max_status_code,
    COUNT(*) FILTER (WHERE host = 'h1') AS h1_rows,
    AVG(attr_key_max_status_code) AS avg_attr_key_max_status_code,
    SUM(CASE WHEN level = 'WARN' THEN status_code_cume_dist ELSE 0 END) AS warn_status_code_cume_dist
FROM with_windows
GROUP BY 1, 2
HAVING debug_events >= 1
ORDER BY warn_status_code_cume_dist DESC;
