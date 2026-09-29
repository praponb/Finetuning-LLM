SELECT YEAR(opened_date) AS opened_year, SORT_ARRAY(COLLECT_SET(city)) AS city_list FROM stores GROUP BY YEAR(opened_date) ORDER BY opened_year;
