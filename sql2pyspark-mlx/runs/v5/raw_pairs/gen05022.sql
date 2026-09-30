SELECT
    id,
    session_id,
    duration_sec,
    event_type,
    event_time,
    page,
    DENSE_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec) AS duration_sec_dense_rank,
    FIRST_VALUE(page) OVER (PARTITION BY event_type ORDER BY event_time, id) AS first_page
FROM web_events;
