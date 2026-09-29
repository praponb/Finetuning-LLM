SELECT region, MIN(opened_date) min_opened_date, SORT_ARRAY(COLLECT_SET(city)) city_list
FROM stores
WHERE region <> 'Central' AND store_name LIKE '%Old%'
GROUP BY region;
