SELECT
    event_type,
    page,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min,
    COUNT(DISTINCT session_id) AS distinct_session_ids,
    SORT_ARRAY(COLLECT_SET(page)) AS page_list,
    COUNT(DISTINCT page) AS distinct_pages,
    BIT_AND(duration_sec) AS duration_sec_bit_and
FROM web_events
GROUP BY GROUPING SETS ((event_type, page), (event_type), ())
