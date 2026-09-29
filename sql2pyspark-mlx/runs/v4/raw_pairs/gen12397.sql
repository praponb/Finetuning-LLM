WITH ranked AS (
    SELECT *
    FROM web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) = 1
)
SELECT
    CASE WHEN duration_sec >= 391 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    SUM(CASE WHEN event_type = 'search' THEN duration_sec ELSE 0 END) AS search_duration_sec,
    COUNT(DISTINCT page) AS distinct_pages,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
    COUNT(CASE WHEN page = '/home' THEN 1 END) AS home_count,
    BIT_AND(duration_sec) AS duration_sec_bit_and
FROM ranked
GROUP BY CASE WHEN duration_sec >= 391 THEN 'HIGH' ELSE 'LOW' END
ORDER BY duration_sec_tier NULLS FIRST;
