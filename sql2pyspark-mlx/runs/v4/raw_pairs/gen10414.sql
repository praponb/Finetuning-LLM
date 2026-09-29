WITH remaining_keys AS (
    SELECT session_id
    FROM web_events
    INTERSECT
    SELECT session_id
    FROM web_events
    WHERE page IS NULL
),
verified AS (
    SELECT s.*
    FROM web_events s
    INNER JOIN remaining_keys k ON s.session_id = k.session_id
)
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
FROM verified
GROUP BY DATE_TRUNC('WEEK', event_time)
ORDER BY duration_sec_stddev NULLS LAST
