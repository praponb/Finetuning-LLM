WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY browser ORDER BY new_group_flag DESC, id) AS new_group_flag_bucket,
        NTILE(5) OVER (PARTITION BY browser ORDER BY session_id DESC, id) AS session_id_bucket
    FROM sessions_sessions
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_windows
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps_2
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.page
    FROM islands s
    LEFT JOIN LATERAL (
            SELECT c.page
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_gaps_3 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag_2
    FROM latest_match
),
islands_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag_2) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM with_gaps_3
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        new_group_flag,
        session_id,
        new_group_flag_bucket,
        session_id_bucket,
        new_group_flag_2,
        island_id_2,
        new_group_flag_2 * island_id AS new_group_flag_2_times_island_id
    FROM islands_sessions
    WHERE started_at > TO_TIMESTAMP('2024-01-23 06:00:00')
      AND browser IS NOT NULL
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            device,
            browser,
            session_id
        FROM cleaned
    ) src
    PIVOT (
        SUM(session_id) AS value_sum,
        COUNT(1) AS cnt
        FOR browser IN ('Chrome', 'Firefox')
    )
)
SELECT
    device,
    Firefox_value_sum,
    CASE WHEN device RLIKE '^(tablet|mobile)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS device_zone
FROM by_type
ORDER BY Firefox_value_sum DESC NULLS FIRST, device DESC
