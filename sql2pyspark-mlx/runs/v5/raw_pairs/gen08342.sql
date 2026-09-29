SELECT page, avg_duration_sec
FROM (
    SELECT page, AVG(duration_sec) AS avg_duration_sec
    FROM web_events
    WHERE page != '/product'
    GROUP BY page
) AS t
WHERE avg_duration_sec < 420
ORDER BY avg_duration_sec;
