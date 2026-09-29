WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('/', event_type, page) AS event_type_page_key
    FROM web_events
    WHERE HOUR(event_time) BETWEEN 11 AND 16
      AND event_time > DATE '2023-04-19'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        session_id,
        event_time,
        duration_sec
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC, id) = 1
),
cleaned_web_events AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(duration_sec, 1) AS duration_sec_rounded,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM deduped
    WHERE event_time >= TIMESTAMP '2023-10-21 23:00:00'
      AND session_id IN (3, 10, 12, 38)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned_web_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_time,
        duration_sec_filled,
        SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
),
latest_web_events AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM islands
    QUALIFY RANK() OVER (PARTITION BY session_id ORDER BY event_time DESC) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(duration_sec_filled, TRUE) OVER (ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec_filled,
        AVG(duration_sec_filled) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_filled_avg_90d,
        ROW_NUMBER() OVER (ORDER BY event_time, id) AS seq_no
    FROM latest_web_events
)
SELECT
    id,
    session_id,
    duration_sec_filled,
    session_id_2,
    last_known_duration_sec_filled,
    duration_sec_filled_avg_90d,
    seq_no
FROM enriched
ORDER BY duration_sec_filled DESC, session_id;
