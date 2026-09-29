WITH with_windows AS (
    SELECT
        id,
        session_id,
        page,
        event_time,
        LEAD(duration_sec, 1, 0) OVER (ORDER BY duration_sec, id) AS next_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_max_30d,
        AVG(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_2_prior
    FROM web_events
)
SELECT
    s.id,
    s.session_id,
    s.page,
    s.event_time,
    s.duration_sec_max_30d,
    s.duration_sec_avg_2_prior,
    (SELECT AVG(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS avg_web_event_duration_sec
FROM with_windows s
ORDER BY page DESC;
