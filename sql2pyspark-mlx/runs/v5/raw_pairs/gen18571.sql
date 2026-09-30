WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, attr_value
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, k) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY host ORDER BY log_ts, id, k) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM lines
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY host ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
matched_keys AS (
    -- Set intersection on keys
    SELECT host, service
    FROM sessionized
    INTERSECT
    SELECT host, service
    FROM app_logs
    WHERE host = 'h2'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    JOIN matched_keys k ON s.host = k.host AND s.service = k.service
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        k,
        attr_value,
        HOUR(log_ts) log_ts_hour,
        DATE_TRUNC('WEEK', log_ts) log_ts_period
    FROM reconciled
    WHERE log_ts >= TO_TIMESTAMP('2025-06-01 07:00:00')
      AND k IS NOT NULL
)
-- Multi-level aggregation
SELECT
    service,
    DATE_TRUNC('WEEK', log_ts_period) AS week_start,
    BIT_AND(latency_ms) AS latency_ms_bit_and,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS duration_min,
    COUNT_IF(k = 'version') AS version_events,
    ROUND(AVG(latency_ms), 2) AS avg_latency_ms
FROM valid
GROUP BY service, DATE_TRUNC('WEEK', log_ts_period)
ORDER BY version_events DESC NULLS LAST;
