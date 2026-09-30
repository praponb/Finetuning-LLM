WITH valid AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM web_events
    WHERE page IS NOT NULL
),
metrics AS (
    SELECT
        *,
        LEAD(duration_sec_filled, 2) OVER (PARTITION BY event_type, page ORDER BY duration_sec_filled, id) AS next_duration_sec_filled,
        SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec
    FROM valid
    WHERE page = '/checkout'
),
scored AS (
    SELECT
        *,
        LAST_VALUE(next_duration_sec_filled) IGNORE NULLS OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_duration_sec_filled,
        LAG(duration_sec_filled, 1) OVER w_grp AS prev_duration_sec_filled,
        NTILE(4) OVER (PARTITION BY page, session_id ORDER BY duration_sec DESC, id) AS duration_sec_bucket
    FROM metrics
    WINDOW w_grp AS (ORDER BY duration_sec_filled, id)
),
remaining_keys AS (
    SELECT page, event_type
    FROM scored
    EXCEPT
    SELECT page, event_type
    FROM web_events
    WHERE session_id IN (9, 37)
),
kept_rows AS (
    SELECT s.*
    FROM scored s
    JOIN remaining_keys k ON s.page = k.page AND s.event_type = k.event_type
)
SELECT
    s.country,
    a.page,
    COUNT(*) AS num_rows,
    AVG(a.duration_sec_bucket) AS avg_duration_sec_bucket,
    SUM(a.running_duration_sec) AS total_running_duration_sec
FROM kept_rows a
JOIN sessions s ON a.session_id = s.id
GROUP BY GROUPING SETS ((s.country, a.page), (s.country), ())
ORDER BY total_running_duration_sec NULLS LAST
