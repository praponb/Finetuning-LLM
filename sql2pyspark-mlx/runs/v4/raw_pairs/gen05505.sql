WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, value
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY latency_ms, id, k) AS dedup_rank
    FROM flattened
    QUALIFY dedup_rank = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT host, level
    FROM deduped
    EXCEPT ALL
    SELECT host, level
    FROM app_logs
    WHERE status_code BETWEEN 344 AND 404
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN remaining_keys k ON s.host = k.host AND s.level = k.level
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        value,
        dedup_rank,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY latency_ms DESC NULLS FIRST) latency_ms_dense_rank,
        DENSE_RANK() OVER w_grp latency_ms_dense_rank_2
    FROM kept_rows
    WINDOW w_grp AS (PARTITION BY level ORDER BY latency_ms NULLS FIRST)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        host,
        log_ts,
        level,
        latency_ms,
        k,
        value,
        dedup_rank,
        latency_ms_dense_rank,
        latency_ms_dense_rank_2,
        INITCAP(TRIM(host)) host_title,
        COALESCE(k, 'NONE') resolved_k
    FROM enriched
    WHERE log_ts > TIMESTAMP '2025-06-01 11:00:00'
      AND k IS NOT NULL
      AND dedup_rank < 1
)
-- Aggregate with conditional metrics
SELECT
    k,
    level,
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    COUNT(*) AS event_count,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes_2,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list_2,
    MAX(log_ts) AS last_log_ts,
    GROUPING(k) AS gid
FROM valid
GROUP BY GROUPING SETS ((k, level, DATE_TRUNC('DAY', log_ts)), (k, level), (k));
