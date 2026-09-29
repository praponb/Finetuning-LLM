SELECT SORT_ARRAY(COLLECT_SET(country)) AS country_list, COUNT(DISTINCT rating) AS unique_countrys FROM suppliers ORDER BY unique_countrys DESC
