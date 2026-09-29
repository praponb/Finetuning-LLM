SELECT
    id,
    page,
    event_time,
    session_id,
    CASE WHEN duration_sec >= 381 THEN 'High' WHEN duration_sec >= 214 THEN 'Medium' ELSE 'Low' END AS duration_sec_band,
    ROUND(duration_sec * 0.9, 2) AS duration_sec_adjusted
FROM web_events;
