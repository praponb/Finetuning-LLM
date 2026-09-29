WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        page,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec ASC, id ASC) AS event_seq,
        LAG(duration_sec, 2) OVER w_ordered AS prev_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY session_id, page ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_avg_30d
    FROM web_events
    WINDOW w_ordered AS (PARTITION BY session_id ORDER BY event_time ASC, id ASC)
)
SELECT
    id,
    event_time,
    event_seq,
    prev_duration_sec,
    duration_sec_avg_30d
FROM with_windows
ORDER BY event_seq DESC NULLS LAST
