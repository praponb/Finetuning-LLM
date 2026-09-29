SELECT browser, COUNT(DISTINCT country) AS distinct_countries, SORT_ARRAY(COLLECT_SET(device)) AS device_list FROM sessions GROUP BY browser
