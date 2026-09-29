SELECT page, SORT_ARRAY(COLLECT_SET(page)) AS page_list FROM web_events GROUP BY page ORDER BY page DESC;
