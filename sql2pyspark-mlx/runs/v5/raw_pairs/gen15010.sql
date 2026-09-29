WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device,
        browser,
        LENGTH(device) AS device_length
    FROM sessions
    WHERE customer_id IS NOT NULL
      AND browser IN ('Safari', 'Chrome', 'Firefox')
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device,
        latest.event_time,
        latest.duration_sec
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.event_time, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY event_time NULLS FIRST, id) AS rn
    FROM with_last
    QUALIFY rn = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_type != 'add_to_cart'
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(duration_sec, 2, 0) OVER (PARTITION BY device ORDER BY rn, id) AS next_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY event_time DESC, id) AS seq_no,
        SUM(duration_sec) OVER w_recent AS cumulative_duration_sec
    FROM not_flagged
    WINDOW w_recent AS (PARTITION BY device ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY device ORDER BY event_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device,
        event_time,
        duration_sec,
        next_duration_sec,
        seq_no,
        SUM(is_new_burst) OVER (PARTITION BY device ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY device ORDER BY event_time, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM islands
),
islands_sessions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device,
        event_time,
        duration_sec,
        seq_no,
        session_id,
        SUM(new_group_flag) OVER (PARTITION BY device ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps_2
),
report AS (
    SELECT
        id,
        device,
        event_time,
        duration_sec,
        seq_no,
        session_id,
        burst_id
    FROM islands_sessions
    ORDER BY id DESC, duration_sec DESC
)
SELECT
    id,
    event_time,
    duration_sec,
    seq_no,
    session_id
FROM report
