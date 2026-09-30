SELECT page, SORT_ARRAY(COLLECT_SET(page)) AS page_list, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values FROM web_events GROUP BY page;
