WITH cleaned AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        CAST(duration_sec AS STRING) AS duration_sec_num,
        COALESCE(page, 'NONE') AS page_clean
    FROM web_events
    WHERE event_time > DATE '2024-05-13'
)
SELECT
    *,
    NTILE(3) OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
    AVG(duration_sec) OVER w_recent AS duration_sec_avg_2_prior
FROM cleaned
WINDOW w_recent AS (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
