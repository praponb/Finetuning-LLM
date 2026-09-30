SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time) rn
    FROM web_events
) t
WHERE rn <= 2;
