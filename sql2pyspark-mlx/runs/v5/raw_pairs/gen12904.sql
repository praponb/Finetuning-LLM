WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_time,
        MONTH(event_time) event_time_month
    FROM web_events
    WHERE page IS NOT DISTINCT FROM '/search'
      AND session_id IN (21, 31)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        event_time,
        event_time_month,
        is_new_burst,
        burst_id,
        DATE_TRUNC('MONTH', event_time) event_time_period
    FROM sessions_web_events
    WHERE event_time >= TIMESTAMP '2023-03-27 09:00:00'
      AND burst_id <> 7
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT
    SELECT id
    FROM web_events
    WHERE page = '/home'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.id = k.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY burst_id) burst_id_dense_rank,
        PERCENT_RANK() OVER (ORDER BY event_time_month NULLS FIRST) event_time_month_percent_rank
    FROM reconciled
)
SELECT
    id,
    event_time,
    event_time_month,
    burst_id,
    event_time_period,
    burst_id_dense_rank,
    event_time_month_percent_rank
FROM metrics
ORDER BY burst_id_dense_rank;
