SELECT * FROM (SELECT page, ROUND(AVG(duration_sec), 2) AS avg_duration_sec, SORT_ARRAY(COLLECT_SET(page)) AS page_list FROM web_events GROUP BY page) AS t WHERE avg_duration_sec > 64
