SELECT MONTH(opened_date) AS opened_month, SORT_ARRAY(COLLECT_SET(city)) AS city_list, SUM(sqft) AS total_sqft FROM stores GROUP BY MONTH(opened_date) ORDER BY opened_month;
