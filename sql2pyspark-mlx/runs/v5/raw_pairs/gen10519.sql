SELECT location, SORT_ARRAY(COLLECT_SET(location)) AS location_list FROM departments GROUP BY 1 ORDER BY location DESC;
