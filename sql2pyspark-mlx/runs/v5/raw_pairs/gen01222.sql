WITH flattened AS (
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
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY host ORDER BY log_ts DESC) AS latest_rank
        FROM flattened
    ) ranked
    WHERE latest_rank = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts, id, key) > INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM deduped
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        host,
        log_ts,
        level,
        key,
        attr_value,
        latest_rank,
        SUM(is_new_burst) OVER (PARTITION BY level ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(island_id) OVER w_hist AS next_island_id,
        LAG(island_id) OVER w_hist AS prev_island_id,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM sessionized
    WINDOW w_hist AS (PARTITION BY level ORDER BY latest_rank, id, key)
),
deduped_app_logs AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key, level ORDER BY next_island_id, id, key) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    host,
    level,
    key,
    latest_rank,
    island_id,
    next_island_id,
    row_seq,
    CASE WHEN key RLIKE '^(trace|version)$' THEN 'EXTENDED' WHEN key RLIKE '^(region)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS key_zone
FROM deduped_app_logs;
