SELECT page, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
FROM web_events
WHERE duration_sec > 365 AND duration_sec <= 449
GROUP BY page
HAVING stddev_duration_sec >= 31
ORDER BY stddev_duration_sec ASC;
