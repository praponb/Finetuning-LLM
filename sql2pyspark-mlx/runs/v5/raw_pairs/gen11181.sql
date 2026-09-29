WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, attr_value
),
with_prev AS (
    SELECT *, LAG(log_ts) OVER (PARTITION BY level ORDER BY log_ts ASC, id ASC, key ASC) AS prev_log_ts
    FROM lines
),
flagged AS (
    SELECT *, CASE WHEN prev_log_ts IS NULL OR log_ts > prev_log_ts + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessionized AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        key,
        SUM(is_new_burst) OVER (PARTITION BY level ORDER BY log_ts ASC, id ASC, key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
filtered AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts,
        LOWER(level) AS level_lower,
        COALESCE(key, 'UNKNOWN') AS resolved_key
    FROM sessionized
    WHERE key IS DISTINCT FROM 'trace'
),
deduped AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY log_ts DESC, id ASC, key ASC) = 1
)
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    key,
    MAX(log_ts) AS ended_at,
    ROUND(SUM(island_id) / NULLIF(COUNT(CASE WHEN level = 'ERROR' THEN 1 END), 0), 2) AS island_id_per_error,
    COUNT_IF(key = 'version') AS version_events,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS span_minutes,
    ROUND(SUM(latency_ms) / NULLIF(COUNT(CASE WHEN level = 'ERROR' THEN 1 END), 0), 2) AS latency_ms_per_error
FROM deduped
GROUP BY ROLLUP (DATE_TRUNC('HOUR', log_ts), key)
