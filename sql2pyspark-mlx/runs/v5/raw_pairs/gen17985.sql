SELECT page, min_duration_sec
FROM (
    SELECT page, MIN(duration_sec) AS min_duration_sec
    FROM web_events
    WHERE event_type = 'click'
    GROUP BY page
) AS agg
WHERE min_duration_sec >= 230
