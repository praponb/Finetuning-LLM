SELECT
    page,
    session_id,
    AVG(duration_sec) AS avg_duration_sec,
    SUM(CASE WHEN event_type = 'purchase' THEN duration_sec ELSE 0 END) AS purchase_duration_sec
FROM web_events
GROUP BY page, session_id
