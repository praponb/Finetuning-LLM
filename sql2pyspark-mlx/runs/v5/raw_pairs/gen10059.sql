WITH enriched AS (
    SELECT
        *,
        LAG(duration_sec, 2) OVER (PARTITION BY page ORDER BY duration_sec, id) AS prev_duration_sec,
        LAG(duration_sec, 2) OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS prev_duration_sec_2,
        SUM(duration_sec) OVER w_hist AS cumulative_duration_sec
    FROM web_events
    WINDOW w_hist AS (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.session_id,
    s.page,
    s.prev_duration_sec_2,
    (SELECT MAX(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS max_web_event_duration_sec
FROM enriched s
ORDER BY id DESC NULLS LAST
